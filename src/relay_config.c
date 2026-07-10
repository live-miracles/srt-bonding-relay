#include "relay_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int parse_uri(const char *uri, char *scheme, size_t scheme_sz, char *host, size_t host_sz,
              int *port, char *query, size_t query_sz) {
    const char *p = strstr(uri, "://");
    if (!p) return -1;

    size_t slen = (size_t)(p - uri);
    if (slen >= scheme_sz) return -1;
    memcpy(scheme, uri, slen);
    scheme[slen] = '\0';
    p += 3;

    const char *qmark = strchr(p, '?');
    const char *host_end = qmark ? qmark : p + strlen(p);
    const char *colon = NULL;
    for (const char *c = host_end; c > p; --c) {
        if (*(c - 1) == ':') {
            colon = c - 1;
            break;
        }
    }
    if (!colon) return -1;

    size_t hlen = (size_t)(colon - p);
    if (hlen >= host_sz) hlen = host_sz - 1;
    memcpy(host, p, hlen);
    host[hlen] = '\0';

    char portbuf[16];
    size_t plen = (size_t)(host_end - colon - 1);
    if (plen == 0 || plen >= sizeof portbuf) return -1;
    memcpy(portbuf, colon + 1, plen);
    portbuf[plen] = '\0';
    *port = atoi(portbuf);
    if (*port <= 0 || *port > 65535) return -1;

    if (query && query_sz > 0) {
        query[0] = '\0';
        if (qmark) {
            size_t qlen = strlen(qmark + 1);
            if (qlen >= query_sz) qlen = query_sz - 1;
            memcpy(query, qmark + 1, qlen);
            query[qlen] = '\0';
        }
    }
    return 0;
}

int get_param(const char *query, const char *key, char *val, size_t val_sz) {
    size_t klen = strlen(key);
    const char *p = query;
    while (p && *p) {
        if (strncmp(p, key, klen) == 0 && p[klen] == '=') {
            const char *v = p + klen + 1;
            const char *end = strchr(v, '&');
            size_t vlen = end ? (size_t)(end - v) : strlen(v);
            if (vlen >= val_sz) vlen = val_sz - 1;
            memcpy(val, v, vlen);
            val[vlen] = '\0';
            char *src = val;
            char *dst = val;
            while (*src) {
                if (src[0] == '%' &&
                    ((src[1] >= '0' && src[1] <= '9') || (src[1] >= 'A' && src[1] <= 'F') ||
                     (src[1] >= 'a' && src[1] <= 'f')) &&
                    ((src[2] >= '0' && src[2] <= '9') || (src[2] >= 'A' && src[2] <= 'F') ||
                     (src[2] >= 'a' && src[2] <= 'f'))) {
                    char hex[3];
                    hex[0] = src[1];
                    hex[1] = src[2];
                    hex[2] = '\0';
                    *dst++ = (char)strtol(hex, NULL, 16);
                    src += 3;
                    continue;
                }
                *dst++ = *src++;
            }
            *dst = '\0';
            return 1;
        }
        p = strchr(p, '&');
        if (p) ++p;
    }
    return 0;
}

static const char *skip_ws(const char *p) {
    while (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t')
        ++p;
    return p;
}

static int json_get_string(const char *json, const char *key, char *out, size_t out_sz) {
    char needle[128];
    snprintf(needle, sizeof needle, "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p = strchr(p + strlen(needle), ':');
    if (!p) return 0;
    p = skip_ws(p + 1);
    if (*p != '"') return 0;
    ++p;

    size_t len = 0;
    while (*p && *p != '"') {
        if (*p == '\\' && p[1]) ++p;
        if (len + 1 >= out_sz) return 0;
        out[len++] = *p++;
    }
    if (*p != '"') return 0;
    out[len] = '\0';
    return 1;
}

static int json_get_int(const char *json, const char *key, int *out) {
    char needle[128];
    snprintf(needle, sizeof needle, "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p = strchr(p + strlen(needle), ':');
    if (!p) return 0;
    p = skip_ws(p + 1);
    char *end = NULL;
    long v = strtol(p, &end, 10);
    if (end == p) return 0;
    *out = (int)v;
    return 1;
}

static void append_param(char *buf, size_t buf_sz, int *first, const char *key, const char *value) {
    size_t used = strlen(buf);
    if (used >= buf_sz) return;
    snprintf(buf + used, buf_sz - used, "%s%s=%s", *first ? "?" : "&", key, value);
    *first = 0;
}

static void append_param_int(char *buf, size_t buf_sz, int *first, const char *key, int value) {
    char tmp[32];
    snprintf(tmp, sizeof tmp, "%d", value);
    append_param(buf, buf_sz, first, key, tmp);
}

static int uri_is_unreserved(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' ||
           c == '.' || c == '_' || c == '~';
}

static int url_encode_component(const char *in, char *out, size_t out_sz) {
    static const char hex[] = "0123456789ABCDEF";
    size_t used = 0;

    for (const unsigned char *p = (const unsigned char *)in; *p; ++p) {
        if (uri_is_unreserved(*p)) {
            if (used + 1 >= out_sz) return -1;
            out[used++] = (char)*p;
        } else {
            if (used + 3 >= out_sz) return -1;
            out[used++] = '%';
            out[used++] = hex[*p >> 4];
            out[used++] = hex[*p & 0x0f];
        }
    }

    if (used >= out_sz) return -1;
    out[used] = '\0';
    return 0;
}

int build_srt_uri(char *out, size_t out_sz, const char *host, int port, const char *passphrase,
                  int include_groupconnect) {
    snprintf(out, out_sz, "srt://%s:%d", host, port);
    int first = 1;
    append_param(out, out_sz, &first, "mode", include_groupconnect ? "listener" : "caller");
    if (include_groupconnect) append_param_int(out, out_sz, &first, "groupconnect", 1);
    append_param(out, out_sz, &first, "transtype", "live");
    append_param_int(out, out_sz, &first, "latency", include_groupconnect ? 240 : 200);
    if (passphrase && passphrase[0]) {
        char encoded_passphrase[sizeof(((file_config_t *)0)->passphrase) * 3];
        if (url_encode_component(passphrase, encoded_passphrase, sizeof encoded_passphrase) != 0) {
            return -1;
        }
        append_param(out, out_sz, &first, "passphrase", encoded_passphrase);
        append_param_int(out, out_sz, &first, "pbkeylen", 16);
    }
    return 0;
}

static int read_file(const char *path, char **buf_out, size_t *len_out) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return -1;
    }
    long len = ftell(f);
    if (len < 0) {
        fclose(f);
        return -1;
    }
    rewind(f);
    char *buf = (char *)calloc((size_t)len + 1, 1);
    if (!buf) {
        fclose(f);
        return -1;
    }
    if (fread(buf, 1, (size_t)len, f) != (size_t)len) {
        free(buf);
        fclose(f);
        return -1;
    }
    fclose(f);
    buf[len] = '\0';
    *buf_out = buf;
    if (len_out) *len_out = (size_t)len;
    return 0;
}

int load_config_file(const char *path, file_config_t *cfg) {
    char *json = NULL;
    if (read_file(path, &json, NULL) != 0) return -1;

    memset(cfg, 0, sizeof *cfg);
    int ok = json_get_string(json, "input_host", cfg->input_host, sizeof cfg->input_host) &&
             json_get_int(json, "input_port", &cfg->input_port) &&
             json_get_string(json, "output_host", cfg->output_host, sizeof cfg->output_host) &&
             json_get_int(json, "output_port", &cfg->output_port) &&
             json_get_int(json, "status_port", &cfg->status_port) &&
             json_get_string(json, "passphrase", cfg->passphrase, sizeof cfg->passphrase);
    free(json);
    if (!ok) return -1;
    if (cfg->input_port <= 0 || cfg->input_port > 65535) return -1;
    if (cfg->output_port <= 0 || cfg->output_port > 65535) return -1;
    if (cfg->status_port <= 0 || cfg->status_port > 65535) return -1;
    /* SRT only accepts passphrases of 10..79 characters. Reject anything
     * else at startup: srt_setsockflag(SRTO_PASSPHRASE) would fail at
     * runtime, leaving the listener rejecting every connection and the
     * output publishing unencrypted. */
    size_t pass_len = strlen(cfg->passphrase);
    if (pass_len > 0 && (pass_len < 10 || pass_len > 79)) {
        fprintf(stderr, "Config passphrase must be empty or 10..79 characters, got %zu\n",
                pass_len);
        return -1;
    }
    return 0;
}
