#ifndef RELAY_CONFIG_H
#define RELAY_CONFIG_H

#include <stddef.h>

typedef struct file_config {
    char input_host[256];
    int input_port;
    char output_host[256];
    int output_port;
    int status_port;
    char passphrase[256];
} file_config_t;

int parse_uri(const char *uri, char *scheme, size_t scheme_sz, char *host, size_t host_sz,
              int *port, char *query, size_t query_sz);
int get_param(const char *query, const char *key, char *val, size_t val_sz);
int build_srt_uri(char *out, size_t out_sz, const char *host, int port, const char *passphrase,
                  int include_groupconnect);
int load_config_file(const char *path, file_config_t *cfg);

#endif
