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
#ifndef _MSG_CHAN_H_
#define _MSG_CHAN_H_

/* A fairly limited message send/receive API.  Inside, there is a thread
 * doing network I/O using libevent.  A remote is an entity in the cluster.
 */

/* */
#define MSG_CHAN_LOGLEVEL_DEBUG  3
#define MSG_CHAN_LOGLEVEL_INFO   2
#define MSG_CHAN_LOGLEVEL_WARN   1
#define MSG_CHAN_LOGLEVEL_FATAL  0

#define MSG_CHAN_MSG_HDR_SIZE (8)
#define MSG_CHAN_MAX_HDR_SIZE (64) /* msg_hdr + user header */

struct msg_chan_sendreq_common {
  TAILQ_ENTRY(msg_chan_sendreq_common) list;
  int msg_size;
  int bytes_sent;
  int user;
};
TAILQ_HEAD(msg_chan_sendreq_list, msg_chan_sendreq_common);

struct msg_chan_sendreq_user {
  struct msg_chan_sendreq_common common; /* msg_chan fills this in */
  int remote; /* remote id */
  const char *body;
  int body_len;
  int hdr_len; /* the user sets this to be the size of the user header */
  union {
    struct {
      char msg_hdr[MSG_CHAN_MSG_HDR_SIZE];
      char user_hdr[MSG_CHAN_MAX_HDR_SIZE-MSG_CHAN_MSG_HDR_SIZE];
    } s;
    char bytes[MSG_CHAN_MAX_HDR_SIZE];
  } hdr;
};

struct msg_chan; /* Opaque.  The user doesn't need to see the inside. */

struct msg_chan_config {
  struct sockaddr_in listen_addr; /* only one for now */
  int listen_backlog;
  
  int ping_interval; /* msec */
  int ping_timeout; /* msec */
  int sndbuf;
  int rcvbuf;
  int reconnect_interval; /* msec */
  int connect_timeout; /* msec */
  int handshake_timeout; /* msec */
  int idle_timeout; /* msec */
  
  /* Callbacks.  msg_chan's net thread calls these functions upon events. */
  void *cb_arg;
  int (*cb_recv_allocate)(void *arg, int remote,
    int body_size, int user_hdr_size, char *user_hdr,
    void **cookie, char **body_buf);
  int (*cb_recv_cancel)(void *arg, void *cookie);
  int (*cb_recv)(void *arg, int remote, void *cookie,
    char *user_hdr, int user_hdr_size);
  void (*cb_send_done)(void *arg, struct msg_chan_sendreq_user *req);
};

struct msg_chan *msg_chan_init(struct msg_chan_config *conf, char *local_name,
  int loglevel);
int msg_chan_destroy(struct msg_chan *ch);

/* Send a message to the specified remote endpoint.  When the message is
 * sent or cancelled, msg_chan calls cb_send_done.
 */
int msg_chan_send(struct msg_chan *ch, struct msg_chan_sendreq_user *req);

/* Associate the remote endpoint's name and its IP address.
 * Returns the remote id for use in msg_chan_send.
 */
int msg_chan_create_remote(struct msg_chan *ch, const char *name,
  struct sockaddr_in *addr);

/* Retrieve the remote endpoint's name and its address */
int msg_chan_get_remote_info(struct msg_chan *ch, int remote,
  char *name, int *name_len, struct sockaddr_in *addr);

#endif /* !defined(_MSG_CHAN_H_) */
