#ifndef CDCLOGFILE_H
#define CDCLOGFILE_H

/* external cdc file function */
bool next_cdclog_path(char *path, char *next_path);
bool cdclog_file_deletable(void);
void cdclog_file_delete(const char *path);

void cdclog_file_write(char *log_ptr, uint32_t log_size, bool dual_write);
void cdclog_file_complete_dual_write(void);
bool cdclog_file_dual_write_finished(void);
int cdclog_file_sync(void);

int cdclog_file_open(char *path);
void cdclog_file_close(void);
void cdclog_file_init(struct default_engine* engine);
void cdclog_file_final(void);
size_t cdclog_file_getsize(void);

void cdclog_get_fsync_lsn(LogSN *lsn);

#endif
