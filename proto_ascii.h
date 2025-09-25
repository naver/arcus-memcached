#ifndef PROTO_ASCII_H
#define PROTO_ASCII_H

#else
void out_string(conn *c, const char *str);
int try_read_command_ascii(conn *c);
void complete_nread_ascii(conn *c);

#endif
