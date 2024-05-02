#include "config.h"
#include <stdio.h>

#include "memcached.h"

static void display(const char *name, size_t size)
{
    printf("%s\t%d\n", name, (int)size);
}

int main(int argc, char **argv)
{
    display("Thread stats", sizeof(struct thread_stats));
    display("Global stats", sizeof(struct mc_stats));
    display("Settings", sizeof(struct settings));
    display("Libevent thread", sizeof(LIBEVENT_THREAD));
    display("Connection", sizeof(conn));

    printf("----------------------------------------\n");

    display("libevent thread cumulative", sizeof(LIBEVENT_THREAD));
    display("Thread stats cumulative\t", sizeof(struct thread_stats));

    printf("----------------------------------------\n");

    display("hash item info\t", sizeof(item_info));
    display("elem item info\t", sizeof(eitem_info));
    display("item attributes\t", sizeof(item_attr));
    display("eflag filter\t", sizeof(eflag_filter));
    display("eflag update\t", sizeof(eflag_update));
    display("Pipe reponse buffer", PIPE_RES_MAX_SIZE);

    return 0;
}
