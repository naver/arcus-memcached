/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_VBUCKET_H
#define MEMCACHED_VBUCKET_H 1

#ifdef __cplusplus
extern "C"
{
#endif

/* number of vbuckets */
#define NUM_VBUCKETS 65536

/* vbucket command */
#define CMD_SET_VBUCKET 0x83
#define CMD_GET_VBUCKET 0x84
#define CMD_DEL_VBUCKET 0x85

/* vbucket state */
enum vbucket_state {
    VBUCKET_STATE_DEAD    = 0,
    VBUCKET_STATE_ACTIVE  = 1,
    VBUCKET_STATE_REPLICA = 2,
    VBUCKET_STATE_PENDING = 3
};

struct vbucket_info {
    int state : 2;
};

union vbucket_info_adapter {
    char c;
    struct vbucket_info v;
};

#if 0 // OLD_CODE
typedef enum {
    active = 1, /**< Actively servicing a vbucket. */
    replica, /**< Servicing a vbucket as a replica only. */
    pending, /**< Pending active. */
    dead /**< Not in use, pending deletion. */
} vbucket_state_t;

#define is_valid_vbucket_state_t(state) \
    (state == active || state == replica || state == pending || state == dead)
#endif

#ifdef __cplusplus
}
#endif
#endif
