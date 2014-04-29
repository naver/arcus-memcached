/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <event.h>

#include "queue.h"
#include "util_common.h"
#include "msg_chan.h"

static int msgchan_loglevel;

// FIXME.  Check event API errrors.

/* All network I/O, event code runs on a single thread.
 * libevent API is not thread safe, so be careful.
 */

/* Message on the wire.
 * Every message starts with msg_hdr.
 */
struct msg_hdr {
  uint32_t size; /* bytes including this header */
  uint16_t type;
  uint16_t user_hdr_size;
#define MSG_HDR_TYPE_HELLO  0
#define MSG_HDR_TYPE_PING   1
#define MSG_HDR_TYPE_PONG   2
#define MSG_HDR_TYPE_USER   3
#define MSG_HDR_TYPE_MAX    3
};
/* FIXME.  The size must be MSG_CHAN_MSG_HDR_SIZE. */
/* FIXME.  Should we have checksum? */

#define MSG_CHAN_MAX_NAME_SIZE 32 /* FIXME */

struct msg_hello {
  struct msg_hdr hdr;
  uint64_t local_timestamp; /* msec */
  char name[MSG_CHAN_MAX_NAME_SIZE];
  uint32_t ip;
  uint32_t port;
};

struct msg_ping {
  struct msg_hdr hdr;
  uint64_t local_timestamp; /* msec */
};
/* msg_pong has the same layout as msg_ping */

struct sendreq_internal {
  struct msg_chan_sendreq_common common;
  /* msg_hello or msg_ping starts here */
};

struct remote {
  int id;
  struct sockaddr_in addr;
  struct tcp_conn *tx;
  struct tcp_conn *rx;
  uint64_t reconnect_msec; /* next time we should try reconnecting */
  uint64_t last_user_msec;
  char name[MSG_CHAN_MAX_NAME_SIZE];
};

struct tcp_conn {
  struct msg_chan *chan;
  struct remote *remote;
  struct sockaddr_in local_addr;
  struct sockaddr_in remote_addr;
  int fd;
  struct event ev_rd;
  struct event ev_wr;
  int state;
#define MSG_CHAN_STATE_CLOSED       0
#define MSG_CHAN_STATE_CONNECTING   1
#define MSG_CHAN_STATE_HANDSHAKE    2
#define MSG_CHAN_STATE_ESTABLISHED  3
  uint64_t connect_timeout_msec;
  uint64_t last_recv_msec;
  uint64_t last_ping_msec;
  uint64_t handshake_timeout_msec;
  int ongoing_pingpong;
  
  /* Send queue of pending messages */
  struct msg_chan_sendreq_list send_list;
  
  /* There are no receive queues.  We read one message from the socket,
   * process it, and repeat.
   */
  char recv_buf[MSG_CHAN_MAX_HDR_SIZE];
  int recv_buf_len;
  int recv_state;
#define MSG_CHAN_RECV_STATE_READ_HEADER    0
#define MSG_CHAN_RECV_STATE_READ_USER_BODY 1
  char *recv_body_buf; /* User-provided via cb_recv_allocate */
  void *recv_cookie;   /* User-provided via cb_recv_allocate */
  int recv_body_rem;
  int recv_user_hdr_len;
  char recv_user_hdr[MSG_CHAN_MAX_HDR_SIZE]; /* copy */

  TAILQ_ENTRY(tcp_conn) list;
};
TAILQ_HEAD(tcp_conn_list, tcp_conn);

struct msg_chan {
  int listen_fd;
  
  struct event_base *ev_base;
  struct event ev_listen;
  struct event ev_clock;
  struct event ev_pipe;
  struct timeval tv_clock;
  struct msg_chan_config conf;
  int pipe_rd, pipe_wr;
  int pipe_notification;
  
  int stop;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  pthread_t tid;
  pthread_attr_t attr;

  struct msg_chan_sendreq_list user_send_list;
  struct tcp_conn_list conn_list;
  uint64_t cur_msec; /* current time, updated by clock_handler */
  
  /* Remotes never disappear. */
#define MAX_REMOTES 1024
  int num_remotes;
  struct remote *remotes[MAX_REMOTES];
  
  char local_name[MSG_CHAN_MAX_NAME_SIZE];
};

const char *state_str[] = {
  "CLOSED",
  "CONNECTING",
  "HANDSHAKE",
  "ESTABLISHED",
};

#define LOCK_CHAN(ch) pthread_mutex_lock(&ch->lock)
#define UNLOCK_CHAN(ch) pthread_mutex_unlock(&ch->lock)

static void tcp_conn_connect(struct msg_chan *ch, struct remote *r);
static void tcp_conn_close(struct tcp_conn *tp);
static void tcp_conn_init_state(struct tcp_conn *tp);
static struct tcp_conn *tcp_conn_new(struct msg_chan *ch);

static int send_internal_msg(struct tcp_conn *tp, int type);
static int send_msg(struct tcp_conn *tp, struct msg_chan_sendreq_common *req);

static void tcp_conn_read_handler(const int fd, const short which, void *arg);
static void tcp_conn_write_handler(const int fd, const short which, void *arg);

static void pipe_read_handler(const int fd, const short which, void *arg);

static int nonblock_connect_check(struct tcp_conn *tp);
static int adjust_socket_buffer_sizes(struct msg_chan *ch, int so);

static int
get_local_addr(struct tcp_conn *tp)
{
  socklen_t addrlen = sizeof(tp->local_addr);
  char buf[INET_ADDRSTRLEN*2];
  const char *ip;
  
  if (0 != getsockname(tp->fd, (struct sockaddr*)&tp->local_addr,
      &addrlen)) {
    ERRLOG(errno, "msg_chan: getsockname failed");
    return -1;
  }
  ip = inet_ntop(AF_INET, (const void*)&tp->local_addr.sin_addr,
    buf, sizeof(buf));
  print_log("msg_chan: channel local_addr=%s:%d",
    (ip ? ip : "null"), ntohs(tp->local_addr.sin_port));
  return 0;
}

static void
free_sendreq(struct msg_chan *ch, struct msg_chan_sendreq_common *req)
{
  if (req->user) {
    /* Ugly.  Restore the original hdr_len... */
    ((struct msg_chan_sendreq_user*)req)->hdr_len -= sizeof(struct msg_hdr);
    ch->conf.cb_send_done(ch->conf.cb_arg, (struct msg_chan_sendreq_user*)req);
  }
  else
    free(req); /* Internally generated message */
}

static void
tcp_conn_close(struct tcp_conn *tp)
{
  struct msg_chan_sendreq_common *req;

  print_log("msg_chan: tcp_conn_close. fd=%d", tp->fd);
  if (tp->fd >= 0) {
    close(tp->fd);
    tp->fd = -1;
  }
  if (tp->state != MSG_CHAN_STATE_CLOSED) {
    event_del(&tp->ev_rd);
    event_del(&tp->ev_wr); /* Harmless if not added previously?  FIXME */
  }
  /* Clear the send queue */
  while ((req = TAILQ_FIRST(&tp->send_list)) != NULL) {
    TAILQ_REMOVE(&tp->send_list, req, list);
    free_sendreq(tp->chan, req);
  }
  if (tp->recv_body_buf) {
    tp->chan->conf.cb_recv_cancel(tp->chan->conf.cb_arg, tp->recv_cookie);
    tp->recv_body_buf = NULL;
  }
  tp->state = MSG_CHAN_STATE_CLOSED;

  if (tp->remote) {
    if (tp->remote->tx == tp)
      tp->remote->tx = NULL;
    else if (tp->remote->rx == tp)
      tp->remote->rx = NULL;
  }
  TAILQ_REMOVE(&tp->chan->conn_list, tp, list);
  free(tp);
}

static int
tcp_conn_read(struct tcp_conn *tp, char *buf, int buf_len)
{
  int s;
  s = read(tp->fd, buf, buf_len);
  if (s < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      /* Try again */
      return 0;
    }
    ERRLOG(errno, "msg_chan: error while reading from the channel connection."
      " fd=%d remote=%s\n", tp->fd, tp->remote ? tp->remote->name : "null");
    return -1;
  }
  else if (s == 0) {
    print_log("msg_chan: the channel connection is closed. fd=%d remote=%s",
      tp->fd, tp->remote ? tp->remote->name : "null");
    return -1;
  }
  return s;
}

static void
tcp_conn_read_handler(const int fd, const short which, void *arg)
{
  struct tcp_conn *tp = arg;
  struct msg_chan *ch = tp->chan;
  struct msg_hdr *h;
  int s, need_reset = 0;
  char *off;

  if (ch->stop) {
    event_base_loopbreak(ch->ev_base);
    return;
  }
  if (msgchan_loglevel >= MSG_CHAN_LOGLEVEL_DEBUG)
    print_log("msg_chan: tcp_conn_read event. fd=%d", fd);
  
  assert(tp->fd == fd);

  /* FIXME.  Rewrite the whole thing.  It's hard to understand. */

  tp->last_recv_msec = ch->cur_msec;
  off = tp->recv_buf;
again:
  if (tp->recv_state == MSG_CHAN_RECV_STATE_READ_HEADER) {
    if (tp->recv_buf_len >= sizeof(*h)) {
      /* Our header (msg_hdr) */
      h = (struct msg_hdr *)off;
      if (h->type > MSG_HDR_TYPE_MAX) {
        print_log("msg_chan: invalid header type."
          " Closing the connection. type=%u remote=%s", h->type,
          tp->remote ? tp->remote->name : "null");
        need_reset = 1;
        goto exit;
      }
      if (sizeof(*h) + h->user_hdr_size > MSG_CHAN_MAX_HDR_SIZE) {
        print_log("msg_chan: the user head is too big."
          " Closing the connection. user_hdr_size=%u remote=%s",
          h->user_hdr_size, tp->remote ? tp->remote->name : "null");
        need_reset = 1;
        goto exit;
      }

      if ((h->type == MSG_HDR_TYPE_USER)) {
        if (tp->recv_buf_len >= sizeof(*h) + h->user_hdr_size) {
          int body_size = h->size - h->user_hdr_size - sizeof(*h);
          char *user_hdr = (char*)(h+1);
          void *cookie;
          char *body_buf;
          
          if (body_size < 0) {
            print_log("msg_chan: body size does not make sense."
              " Closing the connection. size=%u user_hdr_size=%u remote=%s",
              h->size, h->user_hdr_size,
              tp->remote ? tp->remote->name : "null");
            need_reset = 1;
            goto exit;
          }
          if (0 != ch->conf.cb_recv_allocate(ch->conf.cb_arg,
              (tp->remote ? tp->remote->id : -1),
              body_size, h->user_hdr_size, user_hdr, &cookie, &body_buf)) {
            print_log("msg_chan: failed to allocate user body buffer."
              " Closing the connection. remote=%s",
              tp->remote ? tp->remote->name : "null");
            need_reset = 1;
            goto exit;
          }
          tp->recv_body_rem = body_size;
          tp->recv_cookie = cookie;
          tp->recv_body_buf = body_buf;
          tp->recv_state = MSG_CHAN_RECV_STATE_READ_USER_BODY;
          memcpy(tp->recv_user_hdr, user_hdr, h->user_hdr_size);
          tp->recv_user_hdr_len = h->user_hdr_size;

          /* consume bytes in recv_buf */
          off += (sizeof(*h) + h->user_hdr_size);
          tp->recv_buf_len -= (sizeof(*h) + h->user_hdr_size);
          if (tp->recv_buf_len < 0) {
            ERROR_DIE("program error");
          }
          if (tp->remote)
            tp->remote->last_user_msec = ch->cur_msec;
          goto again;
        }
      }
      else {
        /* msg_chan messages */
        if (h->size > MSG_CHAN_MAX_HDR_SIZE) {
          print_log("msg_chan: message is too big. Closing the connection."
            " size=%u remote=%s", h->size,
            tp->remote ? tp->remote->name : "null");
          need_reset = 1;
          goto exit;
        }
        if (tp->recv_buf_len >= h->size) {
          /* Have the complete message.  Process it. */
          
          switch (h->type) {
            case MSG_HDR_TYPE_HELLO:
            {
              struct msg_hello *hello = (struct msg_hello*)h;
              if (tp->state == MSG_CHAN_STATE_HANDSHAKE) {
                if (tp->remote) {
                  hello->name[MSG_CHAN_MAX_NAME_SIZE-1] = '\0';
                  if (0 != strcmp(tp->remote->name, hello->name)) {
                    print_log("msg_chan: unexpected remote name."
                      " Closing the connection. state=%s remote=%s expected=%s",
                      state_str[tp->state], hello->name, tp->remote->name);
                    need_reset = 1;
                    goto exit;
                  }
                }
                else {
                  int id;
                  struct sockaddr_in addr;
                  memset(&addr, 0, sizeof(addr));
                  addr.sin_family = AF_INET;
                  addr.sin_addr.s_addr = hello->ip;
                  addr.sin_port = hello->port;
                  id = msg_chan_create_remote(ch, hello->name, &addr);
                  if (id < 0) {
                    ERROR_DIE("not yet"); // FIXME
                  }
                  else {
                    tp->remote = ch->remotes[id];
                    if (tp->remote->rx) {
                      print_log("msg_chan: established a new rx connection."
                        " Closing the existing rx connection.");
                      tcp_conn_close(tp->remote->rx);
                    }
                    tp->remote->rx = tp;
                  }
                }
                tp->state = MSG_CHAN_STATE_ESTABLISHED;
                print_log("msg_chan: HANDSHAKE => ESTABLISHED");
                if (!TAILQ_EMPTY(&tp->send_list)) {
                  /* Try sending pending messages */
                  event_add(&tp->ev_wr, NULL);
                }
              }
              else {
                print_log("msg_chan: unexpected HELLO message."
                  " Closing the connection. state=%s remote=%s",
                  state_str[tp->state],
                  tp->remote ? tp->remote->name : "null");
                need_reset = 1;
                goto exit;
              }
            }
            break;
            
            case MSG_HDR_TYPE_PING:
            {
              if (msgchan_loglevel >= MSG_CHAN_LOGLEVEL_DEBUG)
                print_log("msg_chan: received PING.");
              if (0 != send_internal_msg(tp, MSG_HDR_TYPE_PONG)) {
                print_log("msg_chan: failed to send PONG."
                  " Closing the connection. state=%s remote=%s",
                  state_str[tp->state],
                  tp->remote ? tp->remote->name : "null");
                need_reset = 1;
                goto exit;
              }
            }
            break;
            
            case MSG_HDR_TYPE_PONG:
            {
              uint64_t cur_msec;
              //print_log("msg_chan: received PONG.");
              gettime(&cur_msec, NULL, NULL);
              tp->ongoing_pingpong = 0;
            }
            break;
            
            default:
            {
              /* Checked above */
              ERROR_DIE("program error");
            }
            break;
          }
          
          /* consume bytes in recv_buf */
          off += h->size;
          tp->recv_buf_len -= h->size;
          if (tp->recv_buf_len < 0) {
            ERROR_DIE("program error");
          }
          goto again;
        }
        else {
          /* Fall through read more bytes */
        }
      }
    }
    
    /* Shift leftover bytes */
    if (off != tp->recv_buf) {
      if (tp->recv_buf_len > 0)
        memmove(tp->recv_buf, off, tp->recv_buf_len);
      off = tp->recv_buf;
    }
    /* We end up calling read at least twice.  That is okay.
     * We could trivially limit it to one call here.
     */
    s = tcp_conn_read(tp, off+tp->recv_buf_len,
      MSG_CHAN_MAX_HDR_SIZE - tp->recv_buf_len - (off - tp->recv_buf));
    if (s == 0) {
      /* Try again */
    }
    else if (s < 0) {
      need_reset = 1;
      goto exit;
    }
    else {
      /* Read more bytes */
      tp->recv_buf_len += s;
      goto again;
    }
  }
  else if (tp->recv_state == MSG_CHAN_RECV_STATE_READ_USER_BODY) {
    /* Copy from recv_buf first */
    if (tp->recv_buf_len > 0) {
      int len = tp->recv_buf_len;
      if (len > tp->recv_body_rem)
        len = tp->recv_body_rem;
      memcpy(tp->recv_body_buf, off, len);
      tp->recv_body_buf += len;
      tp->recv_body_rem -= len;
      tp->recv_buf_len -= len;
      off += len;
      if (tp->remote)
        tp->remote->last_user_msec = ch->cur_msec;
    }

    if (tp->recv_body_rem > 0) {
      s = tcp_conn_read(tp, tp->recv_body_buf, tp->recv_body_rem);
      if (s == 0) {
        /* Try again */
      }
      else if (s < 0) {
        need_reset = 1;
        goto exit;
      }
      else {
        /* Read more bytes */
        tp->recv_body_rem -= s;
        tp->recv_body_buf += s;
        if (tp->remote)
          tp->remote->last_user_msec = ch->cur_msec;
      }
    }

    if (tp->recv_body_rem <= 0) {
      s = ch->conf.cb_recv(ch->conf.cb_arg, (tp->remote ? tp->remote->id : -1),
        tp->recv_cookie, tp->recv_user_hdr, tp->recv_user_hdr_len);
      if (s != 0) {
        need_reset = 1;
        goto exit;
      }
      
      tp->recv_cookie = NULL;
      tp->recv_body_buf = NULL;
      tp->recv_state = MSG_CHAN_RECV_STATE_READ_HEADER;
      goto again;
    }
  }
  else {
    ERROR_DIE("program error");
  }

exit:
  if (need_reset) {
    struct remote *r = tp->remote;
    tcp_conn_close(tp); /* Takes care of recv_body_buf */
    tcp_conn_connect(ch, r);
  }
}

static void
tcp_conn_write_handler(const int fd, const short which, void *arg)
{
  struct tcp_conn *tp = arg;
  struct msg_chan *ch = tp->chan;
  struct remote *r = tp->remote;
  
  if (ch->stop) {
    event_base_loopbreak(ch->ev_base);
    return;
  }

  if (tp->state == MSG_CHAN_STATE_CLOSED) {
    ERROR_DIE("program error");
  }
  else if (tp->state == MSG_CHAN_STATE_CONNECTING) {
    if (0 != nonblock_connect_check(tp)) {
      tcp_conn_close(tp);
      tcp_conn_connect(ch, r);
    }
    else {
      char buf[INET_ADDRSTRLEN*2];
      const char *ip;
      ip = inet_ntop(AF_INET, (const void*)&tp->remote_addr.sin_addr,
        buf, sizeof(buf));
      print_log("msg_chan: successfully connected to the remote. ip=%s:%d"
        " remote=%s",
        (ip ? ip : "null"), ntohs(tp->remote_addr.sin_port),
        tp->remote ? tp->remote->name : "null");
      if (0 != get_local_addr(tp)) {
        tcp_conn_close(tp);
        tcp_conn_connect(ch, r);
      }
      else {
        /* The channel connection is established */
        tcp_conn_init_state(tp);
        if (0 != send_internal_msg(tp, MSG_HDR_TYPE_HELLO)) {
          tcp_conn_close(tp);
          /* Let the timer handler connect. */
        }
      }
    }
  }
  else {
    /* Write pending messages */
    struct msg_chan_sendreq_common *req;
    while ((req = TAILQ_FIRST(&tp->send_list)) != NULL) {
      struct msg_chan_sendreq_user *user = NULL;
      int rem = req->msg_size - req->bytes_sent;
      int s;
      
      if (req->user) {
        struct iovec iov[2];
        int iov_cnt = 0;

        user = (struct msg_chan_sendreq_user *)req;
        if (req->bytes_sent < user->hdr_len) {
          iov[0].iov_base = (void*)(user->hdr.bytes + req->bytes_sent);
          iov[0].iov_len = user->hdr_len - req->bytes_sent;
          iov[1].iov_base = (void*)user->body;
          iov[1].iov_len = user->body_len;
          iov_cnt = 2;
        }
        else if (user->body_len > 0) {
          int body_sent = req->bytes_sent - user->hdr_len;
          iov[0].iov_base = (void*)(user->body + body_sent);
          iov[0].iov_len = user->body_len - body_sent;
          iov_cnt = 1;
        }
        else {
          ERROR_DIE("program error");
        }
        s = writev(tp->fd, iov, iov_cnt);
      }
      else {
        struct sendreq_internal *in = (struct sendreq_internal *)req;
        char *off = ((char*)(in+1)) + req->bytes_sent;
        s = write(tp->fd, off, rem);
      }
      if (s < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          event_add(&tp->ev_wr, NULL);
          return;
        }
        ERRLOG(errno, "msg_chan: error while writing to the remote."
          " Closing the connection. remote=%s",
          r ? r->name : "null");
        tcp_conn_close(tp);
        tcp_conn_connect(ch, r);
        return;
      }
      else if (s == 0) {
        print_log("msg_chan: write returns 0. Closing the connection."
          " remote=%s", r ? r->name : "null");
        tcp_conn_close(tp);
        tcp_conn_connect(ch, r);
        return;
      }
      else {
        req->bytes_sent += s;
        rem -= s;
        if (rem > 0) {
          event_add(&tp->ev_wr, NULL);
          return;
        }
        else {
          TAILQ_REMOVE(&tp->send_list, req, list);
          free_sendreq(tp->chan, req);
        }
      }
    }
  }
}

static void
clock_handler(const int fd, const short which, void *arg)
{
  struct msg_chan *ch = arg;
  struct tcp_conn *tp, *next;
  uint64_t cur_msec;
  int i, num_remotes, need_reset;

  if (ch->stop) {
    event_base_loopbreak(ch->ev_base);
    return;
  }
  gettime(&cur_msec, NULL, NULL);
  ch->cur_msec = cur_msec;

  /* Go through all connections */
  tp = TAILQ_FIRST(&ch->conn_list);
  while (tp) {
    next = TAILQ_NEXT(tp, list);
    need_reset = 0;
    if (tp->state == MSG_CHAN_STATE_HANDSHAKE) {
      if (cur_msec > tp->handshake_timeout_msec) {
        print_log("msg_chan: handshake timed out. waited_msec=%llu",
          (long long unsigned)(tp->handshake_timeout_msec - cur_msec));
        need_reset = 1;
      }
    }
    else if (tp->state == MSG_CHAN_STATE_CONNECTING) {
      if (cur_msec > tp->connect_timeout_msec) {
        print_log("msg_chan: reconnect attempt timed out. waited_msec=%llu",
          (long long unsigned)(cur_msec - tp->connect_timeout_msec));
        need_reset = 1;
      }
    }
    else if (tp->state == MSG_CHAN_STATE_ESTABLISHED) {
      if (tp->ongoing_pingpong) {
        if (cur_msec > tp->last_ping_msec + ch->conf.ping_timeout) {
          print_log("msg_chan: ping timed out. waited_msec=%llu",
            (long long unsigned)(cur_msec - tp->last_ping_msec));
          need_reset = 1;
        }
      }
      else {
        if (cur_msec >= tp->last_ping_msec + ch->conf.ping_interval) {
          tp->ongoing_pingpong = 1;
          tp->last_ping_msec = cur_msec;
          if (0 != send_internal_msg(tp, MSG_HDR_TYPE_PING)) {
            print_log("msg_chan: failed to send ping");
            need_reset = 1;
          }
        }
      }
    }
    else if (tp->state == MSG_CHAN_STATE_CLOSED) {
      ERROR_DIE("program error");
    }

    if (need_reset) {
      struct remote *r = tp->remote;
      tcp_conn_close(tp);
      tcp_conn_connect(ch, r);
    }
    tp = next;
  }

  /* Go through all remotes. */
  LOCK_CHAN(ch);
  num_remotes = ch->num_remotes;
  UNLOCK_CHAN(ch);
  for (i = 0; i < num_remotes; i++) {
    struct remote *r = ch->remotes[i];
    if (r->tx == NULL) {
      tcp_conn_connect(ch, r);
    }
    else if (cur_msec > r->last_user_msec + ch->conf.idle_timeout) {
      /* No user messages sent or received for a while.  Close connections. */
      print_log("msg_chan: idle timeout. Closing the connection.");
      tcp_conn_close(r->tx);
      if (r->rx)
        tcp_conn_close(r->rx);
    }
  }
  
  evtimer_del(&ch->ev_clock);
  evtimer_add(&ch->ev_clock, &ch->tv_clock);
}

static void
listen_read_handler(const int fd, const short which, void *arg)
{
  struct msg_chan *ch = arg;
  struct sockaddr_in addr;
  socklen_t addrlen;
  int s;
  char buf[INET_ADDRSTRLEN*2];
  const char *ip;

  if (ch->stop) {
    event_base_loopbreak(ch->ev_base);
    return;
  }
  assert(fd == ch->listen_fd);

  /* Accept one TCP connection */
  addrlen = sizeof(addr);
  s = accept(fd, (struct sockaddr*)&addr, &addrlen);
  if (s > 0) {
    ip = inet_ntop(AF_INET, (const void*)&addr.sin_addr, buf, sizeof(buf));
    print_log("msg_chan: accepted a new connection. remote=%s:%d fd=%d",
      (ip ? ip : "null"), ntohs(addr.sin_port), s);

    (void)adjust_socket_buffer_sizes(ch, s);
    if (0 != fcntl(s, F_SETFL, O_NONBLOCK)) {
      ERRLOG(errno, "msg_chan: fcntl(O_NONBLOCK) failed.");
      close(s);
    }
    else {
      struct tcp_conn *tp = tcp_conn_new(ch);
      tp->fd = s;
      tp->remote_addr = addr;
      TAILQ_INSERT_HEAD(&ch->conn_list, tp, list);
      tcp_conn_init_state(tp);
      if (0 != send_internal_msg(tp, MSG_HDR_TYPE_HELLO))
        tcp_conn_close(tp);
    }    
  }
  else {
    if (s < 0) {
      if (errno == EMFILE || errno == ENFILE) {
        ERRLOG(errno, "msg_chan: cannot accept tcp connections");
        /* Try again */
      }
      else if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK ||
        errno == ECONNABORTED) {
        /* Try again */
      }
      else if (errno == ENOBUFS || errno == ENOMEM || errno == EPROTO) {
        ERRLOG(errno, "msg_chan: failed to accept tcp connections");
        /* Try again */
      }
      else {
        ERRLOG(errno, "msg_chan: accept failed.");
        /* FIXME.  Close the listen socket or try again? */
      }
    }
  }
}

static void
pipe_read_handler(const int fd, const short which, void *arg)
{
  struct msg_chan *ch = arg;
  int n, s;
  char one_byte[1];
  struct msg_chan_sendreq_list new_list;
  struct msg_chan_sendreq_common *req;

  if (msgchan_loglevel >= MSG_CHAN_LOGLEVEL_DEBUG)
    print_log("msg_chan: pipe_read_handler");
  
  if (ch->stop) {
    event_base_loopbreak(ch->ev_base);
    return;
  }
  assert(fd == ch->pipe_rd);
  TAILQ_INIT(&new_list);
  
  pthread_mutex_lock(&ch->lock);
  if ((n = ch->pipe_notification)) {
    TAILQ_SWAP(&ch->user_send_list, &new_list, msg_chan_sendreq_common, list);
    ch->pipe_notification = 0;
  }
  pthread_mutex_unlock(&ch->lock);
  if (n) {
    s = read(ch->pipe_rd, one_byte, 1);
    if (s != 1) {
      ERROR_DIE("program error");
    }
  }

  while ((req = TAILQ_FIRST(&new_list)) != NULL) {
    struct remote *r;
    struct msg_chan_sendreq_user *user;
    
    TAILQ_REMOVE(&new_list, req, list);
    user = (struct msg_chan_sendreq_user*)req;
    r = ch->remotes[user->remote];
    if (r->tx == NULL) {
      r->last_user_msec = ch->cur_msec;
      tcp_conn_connect(ch, r);
    }
    
    if (r->tx) {
      if (!TAILQ_EMPTY(&r->tx->send_list) ||
        r->tx->state != MSG_CHAN_STATE_ESTABLISHED) {
        TAILQ_INSERT_HEAD(&new_list, req, list);
        TAILQ_CONCAT(&r->tx->send_list, &new_list, list);
        break;
      }
      else if (0 != send_msg(r->tx, req)) {
        /* Put rqst back into the list.  Concat the list to
         * send_list.  chan_close will free everything on the list.
         */
        TAILQ_INSERT_HEAD(&new_list, req, list);
        TAILQ_CONCAT(&r->tx->send_list, &new_list, list);
        tcp_conn_close(r->tx);
        tcp_conn_connect(ch, r);
        break;
      }
    }
    else {
      /* Drop it */
      free_sendreq(ch, req);
    }
  }
}

static int
send_internal_msg(struct tcp_conn *tp, int type)
{
  struct sendreq_internal *in;
  uint64_t msec;

  if (msgchan_loglevel >= MSG_CHAN_LOGLEVEL_DEBUG)
    print_log("msg_chan: send_internal_msg. tp=%p fd=%d type=%d", (void*)tp,
      tp->fd, type);
  
  if (type == MSG_HDR_TYPE_HELLO) {
    struct msg_hello *m;
    
    in = malloc(sizeof(*in) + sizeof(*m));
    memset(in, 0, sizeof(*in) + sizeof(*m));
    m = (struct msg_hello*)(in+1);
    m->hdr.size = sizeof(*m);
    m->hdr.type = type;
    m->hdr.user_hdr_size = 0;
    gettime(&msec, NULL, NULL);
    m->local_timestamp = msec;
    strcpy(m->name, tp->chan->local_name);
    m->ip = tp->chan->conf.listen_addr.sin_addr.s_addr;
    m->port = tp->chan->conf.listen_addr.sin_port;

    in->common.msg_size = m->hdr.size;
    in->common.bytes_sent = 0;
    in->common.user = 0;

    /* FIXME.  Hack.  User messages may already be sitting in the queue. */
    if (tp->state == MSG_CHAN_STATE_HANDSHAKE) {
      if (in->common.msg_size != write(tp->fd, in+1, in->common.msg_size)) {
        /* The very first message.  It has to succeed. */
        return -1;
      }
      return 0;
    }
  }
  else if (type == MSG_HDR_TYPE_PING || type == MSG_HDR_TYPE_PONG) {
    struct msg_ping *m;
    
    in = malloc(sizeof(*in) + sizeof(*m));
    m = (struct msg_ping*)(in+1);
    m->hdr.size = sizeof(*m);
    m->hdr.type = type;
    m->hdr.user_hdr_size = 0;
    gettime(&msec, NULL, NULL);
    m->local_timestamp = msec;

    in->common.msg_size = m->hdr.size;
    in->common.bytes_sent = 0;
    in->common.user = 0;
  }
  else {
    ERROR_DIE("program error");
  }
  return send_msg(tp, &in->common);
}

static int
send_msg(struct tcp_conn *tp, struct msg_chan_sendreq_common *req)
{
  struct sendreq_internal *in = NULL;
  struct msg_chan_sendreq_user *user = NULL;
  int s;

  if (req->user)
    user = (struct msg_chan_sendreq_user*)req;
  else
    in = (struct sendreq_internal*)req;

  if (TAILQ_EMPTY(&tp->send_list)) {
    if (req->user) {
      struct iovec iov[2];
      int iov_cnt ;
      
      iov[0].iov_base = (void*)user->hdr.bytes;
      iov[0].iov_len = user->hdr_len;
      iov_cnt = 1;
      if (user->body_len > 0) {
        iov[1].iov_base = (void*)user->body;
        iov[1].iov_len = user->body_len;
        iov_cnt++;
      }
      s = writev(tp->fd, iov, iov_cnt);
      if (tp->remote) {
        tp->remote->last_user_msec = tp->chan->cur_msec;
      }
    }
    else {
      s = write(tp->fd, in+1, req->msg_size);
    }
    if (s < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        TAILQ_INSERT_TAIL(&tp->send_list, req, list);
        event_add(&tp->ev_wr, NULL);
        return 0;
      }
      ERRLOG(errno, "msg_chan: error while writing to the remote.");
      return -1;
    }
    else if (s == 0) {
      print_log("msg_chan: write returns 0. Closing the connection.");
      return -1;
    }
    else {
      req->bytes_sent += s;
      if (req->bytes_sent < req->msg_size) {
        TAILQ_INSERT_TAIL(&tp->send_list, req, list);
        event_add(&tp->ev_wr, NULL);
      }
      else {
        free_sendreq(tp->chan, req);
      }
    }
  }
  else {
    TAILQ_INSERT_TAIL(&tp->send_list, req, list);
  }
  return 0;
}

static void
tcp_conn_connect(struct msg_chan *ch, struct remote *r)
{
  struct tcp_conn *tp;
  int s;
  char buf[INET_ADDRSTRLEN*2];
  const char *ip;

  if (r == NULL || r->tx != NULL)
    return; /* Still have the tx connection */

  /* Time to reconnect? */
  if (ch->cur_msec < r->reconnect_msec)
    return;
  /* Any user activity? */
  if (ch->cur_msec > r->last_user_msec + ch->conf.idle_timeout)
    return;
  print_log("msg_chan: start connecting. msec=%llu last_user=%llu",
    (long long unsigned)ch->cur_msec, (long long unsigned)r->last_user_msec);
  r->reconnect_msec = ch->cur_msec + ch->conf.reconnect_interval;
  
  s = socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) {
    ERRLOG(errno, "msg_chan: failed to create msg_chan socket.");
    return;
  }
  if (0 != fcntl(s, F_SETFL, O_NONBLOCK)) {
    PERROR_DIE("fcntl(O_NONBLOCK)");
  }
  (void)adjust_socket_buffer_sizes(ch, s);

  tp = tcp_conn_new(ch);
  tp->fd = s;
  tp->remote = r;
  r->tx = tp;
  tp->remote_addr = r->addr;
  TAILQ_INSERT_HEAD(&ch->conn_list, tp, list);

  ip = inet_ntop(AF_INET, (const void*)&tp->remote_addr.sin_addr,
    buf, sizeof(buf));
  print_log("msg_chan: connecting to the remote. remote=%s:%d fd=%d",
    (ip ? ip : "null"), ntohs(tp->remote_addr.sin_port), s);
  
  s = connect(tp->fd, (struct sockaddr*)&tp->remote_addr,
    sizeof(tp->remote_addr));
  if (s < 0) {
    if (errno == EINPROGRESS) {
      /* Check completion later */
      print_log("msg_chan: connect in progress.");
      tp->state = MSG_CHAN_STATE_CONNECTING;
      tp->connect_timeout_msec = ch->cur_msec + ch->conf.connect_timeout;

      event_set(&tp->ev_wr, tp->fd, EV_WRITE,
        tcp_conn_write_handler, tp);
      event_base_set(ch->ev_base, &tp->ev_wr);
      event_add(&tp->ev_wr, NULL);
    }
    else {
      /* Scrap this socket and try connecting again later */
      ERRLOG(errno, "msg_chan: connect failed.");
      tcp_conn_close(tp);
      return;
    }
  }
  else {
    /* Connected right away */
    print_log("msg_chan: successfully connected to the remote.");
    get_local_addr(tp);
    tcp_conn_init_state(tp);
    if (0 != send_internal_msg(tp, MSG_HDR_TYPE_HELLO)) {
      tcp_conn_close(tp);
      /* Do not recurse.  Wait a bit, and the timer handler will connect. */
    }
  }
}

static int
nonblock_connect_check(struct tcp_conn *tp)
{
  int result;
  socklen_t len;
  
  len = sizeof(result);
  if (0 != getsockopt(tp->fd, SOL_SOCKET, SO_ERROR, &result, &len)) {
    ERRLOG(errno, "msg_chan: getsockopt(SO_ERROR) failed.");
    return -1;
  }
  else if (result != 0) {
    ERRLOG(result, "msg_chan: non-blocking connect failed.");
    return -1;
  }
  return 0;
}

static int
adjust_socket_buffer_sizes(struct msg_chan *ch, int so)
{
  int val, snd, rcv, snd_now, rcv_now;
  socklen_t len;
  
  len = sizeof(val);
  if (0 != getsockopt(so, SOL_SOCKET, SO_SNDBUF, &val, &len)) {
    PERROR_DIE("getsockopt(SO_SNDBUF)");
  }
  snd = val;
  
  len = sizeof(val);
  if (0 != getsockopt(so, SOL_SOCKET, SO_RCVBUF, &val, &len)) {
    PERROR_DIE("getsockopt(SO_RCVBUF)");
  }
  rcv = val;

  if (ch->conf.sndbuf > 0) {
    val = ch->conf.sndbuf;
    len = sizeof(val);
    if (0 != setsockopt(so, SOL_SOCKET, SO_SNDBUF, &val, len)) {
      PERROR_DIE("setsockopt(SO_SNDBUF)");
    }
  }
  
  if (ch->conf.rcvbuf > 0) {
    val = ch->conf.rcvbuf;
    len = sizeof(val);
    if (0 != setsockopt(so, SOL_SOCKET, SO_RCVBUF, &val, len)) {
      PERROR_DIE("setsockopt(SO_RCVBUF)");
    }
  }
  
  len = sizeof(val);
  if (0 != getsockopt(so, SOL_SOCKET, SO_SNDBUF, &val, &len)) {
      PERROR_DIE("getsockopt(SO_SNDBUF)");
  }
  snd_now = val;
  
  len = sizeof(val);
  if (0 != getsockopt(so, SOL_SOCKET, SO_RCVBUF, &val, &len)) {
    PERROR_DIE("getsockopt(SO_RCVBUF)");
  }
  rcv_now = val;

  print_log("msg_chan: set socket buffer sizes. fd=%d"
    " sndbuf=%d=>%d rcvbuf=%d=>%d", so, snd, snd_now, rcv, rcv_now);  
  return 0;
}

/* This single thread does all IO work. */
static void *
chan_thread_func(void *arg)
{
  struct msg_chan *ch = arg;
  event_base_loop(ch->ev_base, 0);
  print_log("msg_chan: event loop exited.");
  if (!ch->stop) {
    print_log("msg_chan: loop exited unexpectedly.");
  }
  return NULL;
}

int
msg_chan_send(struct msg_chan *ch, struct msg_chan_sendreq_user *req)
{
  int n, s;
  char one_byte[1];
  struct msg_hdr *h;

  /* Set up the msg_hdr */
  h = (struct msg_hdr*)&req->hdr.s.msg_hdr;
  h->type = MSG_HDR_TYPE_USER;
  h->user_hdr_size = req->hdr_len;
  req->hdr_len = req->hdr_len + sizeof(struct msg_hdr);
  h->size = req->body_len + req->hdr_len;

  /* Set up common */
  req->common.msg_size = h->size;
  req->common.bytes_sent = 0;
  req->common.user = 1;
  
  pthread_mutex_lock(&ch->lock);
  TAILQ_INSERT_TAIL(&ch->user_send_list, &req->common, list);
  if ((n = ch->pipe_notification) == 0)
    ch->pipe_notification = 1;
  pthread_mutex_unlock(&ch->lock);
  
  /* msg_chan owns the new message from now on, regardless of errors */
  if (n == 0) {
    one_byte[0] = 'a';
    s = write(ch->pipe_wr, one_byte, 1);
    if (s != 1) {
      ERRLOG(errno, "msg_chan_send: write returns unexpected result. s=%d", s);
      /* No error returns.  Should we die instead?  FIXME */
    }
  }
  return 0;
}

int
msg_chan_create_remote(struct msg_chan *ch, const char *name,
  struct sockaddr_in *addr)
{
  struct remote *r;
  int i;

  if (strlen(name) >= MSG_CHAN_MAX_NAME_SIZE)
    return -1;

  print_log("msg_chan_create_remote. name=%s", name);
  
  /* Cannot update the remote address */
  LOCK_CHAN(ch);
  for (i = 0; i < ch->num_remotes; i++) {
    r = ch->remotes[i];
    if (0 == strcmp(r->name, name)) {
      UNLOCK_CHAN(ch);
      return i;
    }
  }
  if (ch->num_remotes >= MAX_REMOTES)
    i = -1;
  else {
    r = malloc(sizeof(*r));
    memset(r, 0, sizeof(*r)); /* Let it segfault */
    i = ch->num_remotes++;
    r->id = i;
    strcpy(r->name, name);
    r->addr = *addr;
    ch->remotes[i] = r;
  }
  UNLOCK_CHAN(ch);
  if (i >= 0) {
    char buf[INET_ADDRSTRLEN*2];
    const char *ip;
    ip = inet_ntop(AF_INET, (const void*)&addr->sin_addr, buf, sizeof(buf));
    print_log("msg_chan: created a new remote. remote=%s ip=%s:%d",
      name, (ip ? ip : "null"), ntohs(addr->sin_port));
  }
  return i;
}

int
msg_chan_get_remote_info(struct msg_chan *ch, int remote,
  char *name, int *name_len, struct sockaddr_in *addr)
{
  struct remote *r;
  int len, num_remotes;
  
  LOCK_CHAN(ch);
  num_remotes = ch->num_remotes;
  UNLOCK_CHAN(ch);
  if (remote < 0 || remote >= num_remotes)
    return -1;
  r = ch->remotes[remote];
  *addr = r->addr;
  len = strlen(r->name);
  if (len > *name_len) {
    *name_len = len;
    return -2;
  }
  strcpy(name, r->name);
  return 0;
}

int
msg_chan_destroy(struct msg_chan *ch)
{
  struct tcp_conn *tp;
  void *ret;
  int i;
  
  /* Stop the event thread */
  ch->stop = 1;
  /* Cannot call loopbreak in this thread.  Let the timer event handler
   * call it.  Wait.
   */
  pthread_join(ch->tid, &ret);

  /* Free all connections */
  while ((tp = TAILQ_FIRST(&ch->conn_list))) {
    tcp_conn_close(tp);
  }
  /* Free all remotes */
  for (i = 0; i < ch->num_remotes; i++) {
    free(ch->remotes[i]);
    ch->remotes[i] = NULL;
  }
  ch->num_remotes = 0;
  
  if (ch->listen_fd >= 0) {
    close(ch->listen_fd);
    ch->listen_fd = -1;
  }
  close(ch->pipe_rd);
  close(ch->pipe_wr);

  event_base_free(ch->ev_base);
  
  /* Gone */
  free(ch);
  return 0;
}

static struct tcp_conn *
tcp_conn_new(struct msg_chan *ch)
{
  struct tcp_conn *tp;
  
  tp = malloc(sizeof(*tp));
  memset(tp, 0, sizeof(*tp)); /* Let it segfault */
  TAILQ_INIT(&tp->send_list);
  tp->chan = ch;
  tp->fd = -1;
  tp->state = MSG_CHAN_STATE_CLOSED;
  return tp;
}

static void
tcp_conn_init_state(struct tcp_conn *tp)
{
  tp->state = MSG_CHAN_STATE_HANDSHAKE;
  tp->recv_state = MSG_CHAN_RECV_STATE_READ_HEADER;
  tp->recv_buf_len = 0;
  tp->recv_body_buf = NULL;
  
  event_set(&tp->ev_rd, tp->fd, EV_READ | EV_PERSIST,
    tcp_conn_read_handler, tp);
  event_base_set(tp->chan->ev_base, &tp->ev_rd);
  event_add(&tp->ev_rd, NULL);

  event_set(&tp->ev_wr, tp->fd, EV_WRITE, tcp_conn_write_handler, tp);
  event_base_set(tp->chan->ev_base, &tp->ev_wr);
  
  tp->last_recv_msec = tp->chan->cur_msec;
  tp->last_ping_msec = tp->chan->cur_msec;
  /* Handshake deadline */
  tp->handshake_timeout_msec =
    tp->chan->cur_msec + tp->chan->conf.handshake_timeout;
}

struct msg_chan *
msg_chan_init(struct msg_chan_config *conf, char *local_name, int loglevel)
{
  struct msg_chan *ch;
  int pfd[2];
  int so, val;
  struct sockaddr_in *addr;

  msgchan_loglevel = loglevel;
  ch = malloc(sizeof(*ch));
  if (ch == NULL)
    goto exit;
  memset(ch, 0, sizeof(*ch));

  print_log("msg_chan: initializing the channel.");

  if (strlen(local_name) >= sizeof(ch->local_name)) {
    ERROR_DIE("not yet");
  }
  strcpy(ch->local_name, local_name);
  ch->conf = *conf;
  ch->ev_base = event_init();
  ch->listen_fd = -1;
  ch->pipe_rd = -1;
  ch->pipe_wr = -1;
  TAILQ_INIT(&ch->conn_list);
  TAILQ_INIT(&ch->user_send_list);
  
  /* Set up the listen socket */
  addr = &ch->conf.listen_addr;
  so = socket(AF_INET, SOCK_STREAM, 0);
  if (so < 0) {
    ERRLOG(errno, "msg_chan: failed to create a TCP listen socket.");
    goto exit;
  }
  if (0 != fcntl(so, F_SETFL, O_NONBLOCK)) {
    ERRLOG(errno, "msg_chan: fcntl(O_NONBLOCK) failed.");
    goto exit;
  }
  if (0 != adjust_socket_buffer_sizes(ch, so)) {
    ERRLOG(errno, "msg_chan: adjust_socket_buffer_sizes.");
    goto exit;
  }
  val = 1;
  if (0 != setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val))) {
    ERRLOG(errno, "msg_chan: setsockopt(SO_REUSEADDR) failed.");
    goto exit;
  }
  if (0 != bind(so, (const struct sockaddr*)addr, sizeof(*addr))) {
    char buf[INET_ADDRSTRLEN*2];
    const char *ip;
    ip = inet_ntop(AF_INET, (const void*)&addr->sin_addr, buf, sizeof(buf));
    ERRLOG(errno, "msg_chan: bind failed. addr=%s:%d",
      (ip ? ip : "null"), ntohs(addr->sin_port));
    goto exit;
  }
  if (0 != listen(so, ch->conf.listen_backlog)) {
    ERRLOG(errno, "msg_chan: listen failed.");
    goto exit;
  }
  
  ch->listen_fd = so;
  event_set(&ch->ev_listen, ch->listen_fd, EV_READ | EV_PERSIST,
    listen_read_handler, ch);
  event_base_set(ch->ev_base, &ch->ev_listen);
  event_add(&ch->ev_listen, NULL);

  /* Clock tick */
  ch->tv_clock.tv_sec = 0;
  ch->tv_clock.tv_usec = 10000; /* FIXME.  10msec */
  evtimer_set(&ch->ev_clock, clock_handler, ch);
  event_base_set(ch->ev_base, &ch->ev_clock);
  evtimer_add(&ch->ev_clock, &ch->tv_clock);
  gettime(&ch->cur_msec, NULL, NULL);

  /* Pipe to wake up our thread */
  if (0 != pipe(pfd))
    goto exit;
  ch->pipe_rd = pfd[0];
  ch->pipe_wr = pfd[1];
  event_set(&ch->ev_pipe, ch->pipe_rd, EV_READ | EV_PERSIST,
    pipe_read_handler, ch);
  event_base_set(ch->ev_base, &ch->ev_pipe);
  event_add(&ch->ev_pipe, NULL);
  
  pthread_mutex_init(&ch->lock, NULL);
  pthread_cond_init(&ch->cond, NULL);
  pthread_attr_init(&ch->attr);
  pthread_attr_setscope(&ch->attr, PTHREAD_SCOPE_SYSTEM);
  pthread_create(&ch->tid, &ch->attr, chan_thread_func, (void*)ch);

  return ch;

exit:
  /* Do not worry much about cleaning up.  If the application cannot create
   * msg_chan, it won't be able to proceed anyway.
   */
  if (ch) {
    if (ch->pipe_rd >= 0)
      close(ch->pipe_rd);
    if (ch->pipe_wr >= 0)
      close(ch->pipe_wr);
    if (ch->ev_base)
      event_base_free(ch->ev_base);
    free(ch);
  }
  return NULL;
}
