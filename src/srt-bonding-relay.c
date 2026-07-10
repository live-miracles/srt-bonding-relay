/*
 * srt-bonding-relay.c
 *
 * Long-running SRT bonding ingress relay.
 *
 * Listens on one encoder-facing SRT port with SRTO_GROUPCONNECT enabled,
 * accepts bonded/redundant SRT groups, and forwards the deduplicated MPEG-TS
 * payload into a downstream SRT output. The incoming caller streamid is
 * copied to the outgoing SRT connection, so a single listener can serve many
 * logical streams.
 *
 * Usage:
 *   srt-bonding-relay <config.json>
 *   srt-bonding-relay <srt-input-uri> <srt-output-uri>
 *
 * Example JSON:
 *   {
 *     "input_host": "0.0.0.0",
 *     "input_port": 10081,
 *     "output_host": "127.0.0.1",
 *     "output_port": 10080,
 *     "status_port": 8081,
 *     "passphrase": "secret-value"
 *   }
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <srt/srt.h>

#include "relay_config.h"

#define CHUNK 1456
#define LISTEN_BACKLOG 64
#define MAX_EVENTS 16
#define MAX_ACTIVE_SESSIONS 256
#define MAX_GROUP_MEMBERS 16
#define LAST_ERROR_MAX 512
#define DUPLICATE_TAKEOVER_WAIT_MS 5000
#define DUPLICATE_TAKEOVER_STALE_MS 5000

#ifndef RELAY_VERSION
#define RELAY_VERSION "dev"
#endif

static const char *RELAY_VERSION_STRING = RELAY_VERSION;
static const int RETRY_DELAYS_MS[] = {1000, 2000, 4000, 8000, 16000};

typedef struct relay_leg_state {
    char ip[INET6_ADDRSTRLEN];
    int port;
    int member_state;
    int stats_valid;
    long long recv_packets_total;
    long long recv_unique_packets_total;
    int recv_loss_total;
    int recv_drop_total;
    int retrans_total;
    double rtt_ms;
    int rtt_valid;
} relay_leg_state_t;

typedef struct relay_stream_state {
    char streamid[1024];
    int input_active;
    int output_connected;
    int retry_failures;
    long long last_input_packet_at_ms;
    long long last_input_packet_mono_ms;
    long long forwarded_packets;
    long long forwarded_bytes;
    long long last_packet_at_ms;
    long long recv_packets_total;
    int recv_packets_total_valid;
    long long recv_unique_packets_total;
    int recv_loss_total;
    int recv_drop_total;
    int retrans_total;
    double input_rtt_ms;
    int input_rtt_valid;
    double output_rtt_ms;
    int output_rtt_valid;
    long long output_sent_packets_total;
    int output_send_loss_total;
    int output_send_drop_total;
    int output_retrans_total;
    relay_leg_state_t legs[MAX_GROUP_MEMBERS];
    int leg_count;
    long long last_stats_at_ms;
    char last_error[LAST_ERROR_MAX];
    long long last_error_at_ms;
} relay_stream_state_t;

typedef struct active_session {
    int in_use;
    char streamid[1024];
    SRTSOCKET input_sock;
    SRTSOCKET output_sock;
} active_session_t;

typedef struct relay_session_slot {
    relay_stream_state_t state;
    active_session_t active;
    pthread_mutex_t state_mu;
} relay_session_slot_t;

/* Locking rules for g_sessions[i].state:
 *  - g_sessions_mu guards slot ownership: state.streamid is only written
 *    (claimed or cleared) while holding BOTH g_sessions_mu and the slot's
 *    state_mu, so it may be read under either lock alone.
 *  - Every other state field is guarded by the slot's state_mu only, so
 *    per-packet bookkeeping from one session never contends with other
 *    sessions on a process-wide lock.
 *  - g_last_error is guarded by its own g_last_error_mu.
 *  - Lock order where nested: g_sessions_mu -> state_mu.
 * The active (socket tracking) fields stay under g_session_threads_mu. */
static volatile sig_atomic_t g_running = 1;
static pthread_mutex_t g_sessions_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_session_threads_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_session_threads_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_last_error_mu = PTHREAD_MUTEX_INITIALIZER;
static char g_last_error[512];
static relay_session_slot_t g_sessions[MAX_ACTIVE_SESSIONS];
static int g_active_session_threads = 0;
static long long g_started_at_ms = 0;
static int g_status_port = 8081;

static void on_signal(int s) {
    (void)s;
    g_running = 0;
}

static int install_signal_handlers(void) {
    struct sigaction stop_sa;
    memset(&stop_sa, 0, sizeof stop_sa);
    stop_sa.sa_handler = on_signal;
    sigemptyset(&stop_sa.sa_mask);

    if (sigaction(SIGINT, &stop_sa, NULL) < 0) return -1;
    if (sigaction(SIGTERM, &stop_sa, NULL) < 0) return -1;

    struct sigaction pipe_sa;
    memset(&pipe_sa, 0, sizeof pipe_sa);
    pipe_sa.sa_handler = SIG_IGN;
    sigemptyset(&pipe_sa.sa_mask);
    if (sigaction(SIGPIPE, &pipe_sa, NULL) < 0) return -1;

    return 0;
}

static void init_session_tracking(void) {
    pthread_mutex_lock(&g_session_threads_mu);
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        memset(&g_sessions[i].active, 0, sizeof g_sessions[i].active);
        g_sessions[i].active.input_sock = SRT_INVALID_SOCK;
        g_sessions[i].active.output_sock = SRT_INVALID_SOCK;
        pthread_mutex_init(&g_sessions[i].state_mu, NULL);
    }
    g_active_session_threads = 0;
    pthread_mutex_unlock(&g_session_threads_mu);
}

static int register_session_thread(SRTSOCKET conn) {
    int slot = -1;

    pthread_mutex_lock(&g_session_threads_mu);
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (!g_sessions[i].active.in_use) {
            slot = i;
            memset(&g_sessions[i].active, 0, sizeof g_sessions[i].active);
            g_sessions[i].active.in_use = 1;
            g_sessions[i].active.input_sock = conn;
            g_sessions[i].active.output_sock = SRT_INVALID_SOCK;
            g_active_session_threads++;
            break;
        }
    }
    pthread_mutex_unlock(&g_session_threads_mu);

    return slot;
}

static void set_tracked_session_streamid(int slot, const char *streamid) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;

    pthread_mutex_lock(&g_session_threads_mu);
    if (g_sessions[slot].active.in_use) {
        g_sessions[slot].active.streamid[0] = '\0';
        if (streamid) {
            strncpy(g_sessions[slot].active.streamid, streamid,
                    sizeof g_sessions[slot].active.streamid - 1);
            g_sessions[slot].active.streamid[sizeof g_sessions[slot].active.streamid - 1] = '\0';
        }
    }
    pthread_mutex_unlock(&g_session_threads_mu);
}

static void set_tracked_session_output_socket(int slot, SRTSOCKET sock) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;

    pthread_mutex_lock(&g_session_threads_mu);
    if (g_sessions[slot].active.in_use) {
        g_sessions[slot].active.output_sock = sock;
    }
    pthread_mutex_unlock(&g_session_threads_mu);
}

static SRTSOCKET take_tracked_input_socket(int slot, SRTSOCKET expected) {
    SRTSOCKET sock = SRT_INVALID_SOCK;

    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return SRT_INVALID_SOCK;

    pthread_mutex_lock(&g_session_threads_mu);
    if (g_sessions[slot].active.in_use && g_sessions[slot].active.input_sock == expected) {
        sock = g_sessions[slot].active.input_sock;
        g_sessions[slot].active.input_sock = SRT_INVALID_SOCK;
    }
    pthread_mutex_unlock(&g_session_threads_mu);

    return sock;
}

static SRTSOCKET take_tracked_output_socket(int slot, SRTSOCKET expected) {
    SRTSOCKET sock = SRT_INVALID_SOCK;

    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return SRT_INVALID_SOCK;

    pthread_mutex_lock(&g_session_threads_mu);
    if (g_sessions[slot].active.in_use && g_sessions[slot].active.output_sock == expected) {
        sock = g_sessions[slot].active.output_sock;
        g_sessions[slot].active.output_sock = SRT_INVALID_SOCK;
    }
    pthread_mutex_unlock(&g_session_threads_mu);

    return sock;
}

static int tracked_socket_is_current(int slot, SRTSOCKET input_sock, SRTSOCKET output_sock) {
    int current = 0;

    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return 0;

    pthread_mutex_lock(&g_session_threads_mu);
    if (g_sessions[slot].active.in_use &&
        (input_sock == SRT_INVALID_SOCK || g_sessions[slot].active.input_sock == input_sock) &&
        (output_sock == SRT_INVALID_SOCK || g_sessions[slot].active.output_sock == output_sock)) {
        current = 1;
    }
    pthread_mutex_unlock(&g_session_threads_mu);

    return current;
}

static void close_tracked_sessions_for_streamid(const char *streamid, int except_slot) {
    SRTSOCKET sockets[MAX_ACTIVE_SESSIONS * 2];
    int count = 0;

    if (!streamid || !streamid[0]) return;

    pthread_mutex_lock(&g_session_threads_mu);
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (!g_sessions[i].active.in_use || i == except_slot) continue;
        if (strcmp(g_sessions[i].active.streamid, streamid) != 0) continue;

        if (g_sessions[i].active.input_sock != SRT_INVALID_SOCK) {
            sockets[count++] = g_sessions[i].active.input_sock;
            g_sessions[i].active.input_sock = SRT_INVALID_SOCK;
        }
        if (g_sessions[i].active.output_sock != SRT_INVALID_SOCK) {
            sockets[count++] = g_sessions[i].active.output_sock;
            g_sessions[i].active.output_sock = SRT_INVALID_SOCK;
        }
    }
    pthread_mutex_unlock(&g_session_threads_mu);

    for (int i = 0; i < count; ++i) {
        srt_close(sockets[i]);
    }
}

static void unregister_session_thread(int slot) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;

    pthread_mutex_lock(&g_session_threads_mu);
    if (g_sessions[slot].active.in_use) {
        memset(&g_sessions[slot].active, 0, sizeof g_sessions[slot].active);
        g_sessions[slot].active.input_sock = SRT_INVALID_SOCK;
        g_sessions[slot].active.output_sock = SRT_INVALID_SOCK;
        if (g_active_session_threads > 0) g_active_session_threads--;
        pthread_cond_broadcast(&g_session_threads_cond);
    }
    pthread_mutex_unlock(&g_session_threads_mu);
}

static void close_active_session_sockets(void) {
    SRTSOCKET sockets[MAX_ACTIVE_SESSIONS * 2];
    int count = 0;

    pthread_mutex_lock(&g_session_threads_mu);
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (!g_sessions[i].active.in_use) continue;
        if (g_sessions[i].active.input_sock != SRT_INVALID_SOCK) {
            sockets[count++] = g_sessions[i].active.input_sock;
            g_sessions[i].active.input_sock = SRT_INVALID_SOCK;
        }
        if (g_sessions[i].active.output_sock != SRT_INVALID_SOCK) {
            sockets[count++] = g_sessions[i].active.output_sock;
            g_sessions[i].active.output_sock = SRT_INVALID_SOCK;
        }
    }
    pthread_mutex_unlock(&g_session_threads_mu);

    for (int i = 0; i < count; ++i) {
        srt_close(sockets[i]);
    }
}

static void wait_for_session_threads(void) {
    pthread_mutex_lock(&g_session_threads_mu);
    while (g_active_session_threads > 0) {
        pthread_cond_wait(&g_session_threads_cond, &g_session_threads_mu);
    }
    pthread_mutex_unlock(&g_session_threads_mu);
}

typedef struct relay_config {
    char out_query[1024];
    struct sockaddr_storage out_addr;
    socklen_t out_addrlen;
} relay_config_t;

typedef struct session_args {
    SRTSOCKET conn;
    const relay_config_t *cfg;
    int tracker_slot;
} session_args_t;

/* Wall clock; used only for timestamps exposed via the status API. */
static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + (long long)ts.tv_nsec / 1000000LL;
}

/* Monotonic clock for internal timers (retry backoff, duplicate-takeover
 * staleness, stats throttling), which must not fire early or stall when
 * NTP steps the wall clock. */
static long long now_mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + (long long)ts.tv_nsec / 1000000LL;
}

static void json_write_escaped(FILE *f, const char *s) {
    for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        switch (*p) {
        case '\\':
            fputs("\\\\", f);
            break;
        case '"':
            fputs("\\\"", f);
            break;
        case '\n':
            fputs("\\n", f);
            break;
        case '\r':
            fputs("\\r", f);
            break;
        case '\t':
            fputs("\\t", f);
            break;
        default:
            if (*p < 0x20) {
                fprintf(f, "\\u%04x", *p);
            } else {
                fputc(*p, f);
            }
            break;
        }
    }
}

static void set_last_errorf(const char *fmt, ...) {
    pthread_mutex_lock(&g_last_error_mu);
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_last_error, sizeof g_last_error, fmt, ap);
    va_end(ap);
    pthread_mutex_unlock(&g_last_error_mu);
}

static void set_stream_errorf(int slot, const char *fmt, ...) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_sessions[slot].state.last_error, sizeof g_sessions[slot].state.last_error, fmt, ap);
    va_end(ap);
    g_sessions[slot].state.last_error_at_ms = now_ms();
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
}

static long long max_ll(long long a, long long b) {
    return a > b ? a : b;
}

static void format_leg_addr(const struct sockaddr_storage *ss, char *ip_out, size_t ip_out_len,
                            int *port_out) {
    ip_out[0] = '\0';
    *port_out = 0;
    if (ss->ss_family == AF_INET) {
        const struct sockaddr_in *sin = (const struct sockaddr_in *)ss;
        inet_ntop(AF_INET, &sin->sin_addr, ip_out, ip_out_len);
        *port_out = ntohs(sin->sin_port);
    } else if (ss->ss_family == AF_INET6) {
        const struct sockaddr_in6 *sin6 = (const struct sockaddr_in6 *)ss;
        inet_ntop(AF_INET6, &sin6->sin6_addr, ip_out, ip_out_len);
        *port_out = ntohs(sin6->sin6_port);
    }
}

static const char *member_state_str(int member_state) {
    switch (member_state) {
    case SRT_GST_PENDING:
        return "pending";
    case SRT_GST_IDLE:
        return "idle";
    case SRT_GST_RUNNING:
        return "running";
    case SRT_GST_BROKEN:
        return "broken";
    default:
        return "unknown";
    }
}

static int max_int(int a, int b) {
    return a > b ? a : b;
}

static int claim_stream_state(const char *streamid, int preferred_slot) {
    if (!streamid || !streamid[0]) return -1;
    pthread_mutex_lock(&g_sessions_mu);
    int slot = -1;
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (g_sessions[i].state.streamid[0] &&
            strcmp(g_sessions[i].state.streamid, streamid) == 0) {
            slot = -2;
            break;
        }
    }
    if (slot == -1) {
        if (preferred_slot >= 0 && preferred_slot < MAX_ACTIVE_SESSIONS &&
            !g_sessions[preferred_slot].state.streamid[0]) {
            slot = preferred_slot;
        }
        if (slot >= 0) {
            pthread_mutex_lock(&g_sessions[slot].state_mu);
            memset(&g_sessions[slot].state, 0, sizeof g_sessions[slot].state);
            strncpy(g_sessions[slot].state.streamid, streamid,
                    sizeof g_sessions[slot].state.streamid - 1);
            g_sessions[slot].state.streamid[sizeof g_sessions[slot].state.streamid - 1] = '\0';
            g_sessions[slot].state.input_active = 1;
            g_sessions[slot].state.last_input_packet_at_ms = now_ms();
            g_sessions[slot].state.last_input_packet_mono_ms = now_mono_ms();
            pthread_mutex_unlock(&g_sessions[slot].state_mu);
        }
    }
    pthread_mutex_unlock(&g_sessions_mu);
    return slot;
}

static int stream_input_age_ms(const char *streamid, long long now, long long *age_ms) {
    if (!streamid || !streamid[0]) return -1;

    int found = 0;
    long long last_input_at = 0;

    pthread_mutex_lock(&g_sessions_mu);
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (!g_sessions[i].state.streamid[0]) continue;
        if (strcmp(g_sessions[i].state.streamid, streamid) != 0) continue;
        found = 1;
        pthread_mutex_lock(&g_sessions[i].state_mu);
        last_input_at = g_sessions[i].state.last_input_packet_mono_ms;
        pthread_mutex_unlock(&g_sessions[i].state_mu);
        break;
    }
    pthread_mutex_unlock(&g_sessions_mu);

    if (!found) return -1;
    if (last_input_at <= 0) last_input_at = now;
    if (age_ms) *age_ms = now - last_input_at;
    return 0;
}

static void remove_stream_state(int slot) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;
    pthread_mutex_lock(&g_sessions_mu);
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    memset(&g_sessions[slot].state, 0, sizeof g_sessions[slot].state);
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
    pthread_mutex_unlock(&g_sessions_mu);
}

static void set_stream_output_connected(int slot, int connected) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    g_sessions[slot].state.output_connected = connected;
    if (connected) {
        g_sessions[slot].state.retry_failures = 0;
        g_sessions[slot].state.last_error[0] = '\0';
        g_sessions[slot].state.last_error_at_ms = 0;
    }
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
}

static void increment_stream_retry_failures(int slot) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    g_sessions[slot].state.retry_failures++;
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
}

static int get_stream_retry_failures(int slot) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return 0;
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    int failures = g_sessions[slot].state.retry_failures;
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
    return failures;
}

static int get_retry_delay_ms(int failures) {
    int idx = failures - 1;
    if (idx < 0) idx = 0;
    int max_idx = (int)(sizeof RETRY_DELAYS_MS / sizeof RETRY_DELAYS_MS[0]) - 1;
    if (idx > max_idx) idx = max_idx;
    return RETRY_DELAYS_MS[idx];
}

static void record_stream_input_progress(int slot) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    g_sessions[slot].state.last_input_packet_at_ms = now_ms();
    g_sessions[slot].state.last_input_packet_mono_ms = now_mono_ms();
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
}

static void record_stream_forward_progress(int slot, int bytes) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS || bytes <= 0) return;
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    g_sessions[slot].state.forwarded_packets++;
    g_sessions[slot].state.forwarded_bytes += bytes;
    g_sessions[slot].state.last_packet_at_ms = now_ms();
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
}

static void update_stream_srt_counters(int slot, int tracker_slot, SRTSOCKET in_sock,
                                       SRTSOCKET out_sock, int force) {
    if (slot < 0 || slot >= MAX_ACTIVE_SESSIONS) return;

    long long now = now_mono_ms();
    pthread_mutex_lock(&g_sessions[slot].state_mu);
    long long last = g_sessions[slot].state.last_stats_at_ms;
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
    if (!force && last > 0 && now - last < 1000) return;

    SRT_TRACEBSTATS in_stats;
    memset(&in_stats, 0, sizeof in_stats);
    int have_in = 0;

    int is_group_sock = in_sock != SRT_INVALID_SOCK && (in_sock & SRTGROUP_MASK);
    int have_out = 0;
    SRT_TRACEBSTATS out_stats;
    memset(&out_stats, 0, sizeof out_stats);
    relay_leg_state_t legs[MAX_GROUP_MEMBERS];
    int leg_count = 0;
    memset(legs, 0, sizeof legs);

    pthread_mutex_lock(&g_session_threads_mu);
    int tracker_valid = tracker_slot >= 0 && tracker_slot < MAX_ACTIVE_SESSIONS;
    int can_read_in = tracker_valid && in_sock != SRT_INVALID_SOCK &&
                      g_sessions[tracker_slot].active.in_use &&
                      g_sessions[tracker_slot].active.input_sock == in_sock;
    int can_read_out = tracker_valid && out_sock != SRT_INVALID_SOCK &&
                       g_sessions[tracker_slot].active.in_use &&
                       g_sessions[tracker_slot].active.output_sock == out_sock;
    pthread_mutex_unlock(&g_session_threads_mu);

    /* The stats calls below run outside g_session_threads_mu: holding it
     * across up to 1 + MAX_GROUP_MEMBERS srt_bstats() calls per session per
     * second serialized every session against the accept path
     * (register_session_thread) and all socket-handoff operations. The
     * tradeoff is a benign race: a concurrent takeover/shutdown may
     * srt_close() one of these sockets after the ownership check above, in
     * which case libsrt fails the call on the stale id (socket ids are not
     * reused) and the sample is skipped - same handling as any other stats
     * failure. The sockets are never closed by this session's own thread
     * while it is in this function. */
    if (can_read_in) {
        have_in = srt_bstats(in_sock, &in_stats, 0) != SRT_ERROR;

        if (is_group_sock) {
            SRT_SOCKGROUPDATA members[MAX_GROUP_MEMBERS];
            size_t member_count = MAX_GROUP_MEMBERS;
            memset(members, 0, sizeof members);
            if (srt_group_data(in_sock, members, &member_count) != SRT_ERROR) {
                for (size_t i = 0; i < member_count; ++i) {
                    if (members[i].id == SRT_INVALID_SOCK) continue;
                    if (members[i].memberstate == SRT_GST_BROKEN) continue;

                    SRT_TRACEBSTATS member_stats;
                    memset(&member_stats, 0, sizeof member_stats);
                    int have_member_stats =
                        srt_bstats(members[i].id, &member_stats, 0) != SRT_ERROR;

                    if (leg_count < MAX_GROUP_MEMBERS) {
                        relay_leg_state_t *leg = &legs[leg_count++];
                        format_leg_addr(&members[i].peeraddr, leg->ip, sizeof leg->ip, &leg->port);
                        leg->member_state = members[i].memberstate;
                        leg->stats_valid = have_member_stats;
                        if (have_member_stats) {
                            leg->rtt_ms = member_stats.msRTT;
                            leg->rtt_valid = member_stats.msRTT > 0.0;
                            leg->recv_packets_total = member_stats.pktRecvTotal;
                            leg->recv_unique_packets_total = member_stats.pktRecvUniqueTotal;
                            leg->recv_loss_total = member_stats.pktRcvLossTotal;
                            leg->recv_drop_total = member_stats.pktRcvDropTotal;
                            leg->retrans_total = member_stats.pktRcvRetrans;
                        }
                    }
                }
            }
        }
    }

    if (can_read_out) {
        have_out = srt_bstats(out_sock, &out_stats, 0) != SRT_ERROR;
    }

    pthread_mutex_lock(&g_sessions[slot].state_mu);
    if (have_in) {
        g_sessions[slot].state.input_rtt_ms = in_stats.msRTT;
        g_sessions[slot].state.input_rtt_valid = in_stats.msRTT > 0.0;
        g_sessions[slot].state.recv_packets_total_valid =
            !(is_group_sock && in_stats.pktRecvTotal == 0 && in_stats.pktRecvUniqueTotal > 0);
        g_sessions[slot].state.recv_packets_total =
            max_ll(g_sessions[slot].state.recv_packets_total, in_stats.pktRecvTotal);
        g_sessions[slot].state.recv_unique_packets_total =
            max_ll(g_sessions[slot].state.recv_unique_packets_total, in_stats.pktRecvUniqueTotal);
        g_sessions[slot].state.recv_loss_total =
            max_int(g_sessions[slot].state.recv_loss_total, in_stats.pktRcvLossTotal);
        g_sessions[slot].state.recv_drop_total =
            max_int(g_sessions[slot].state.recv_drop_total, in_stats.pktRcvDropTotal);
        g_sessions[slot].state.retrans_total =
            max_int(g_sessions[slot].state.retrans_total, in_stats.pktRcvRetrans);
    }
    if (have_out) {
        g_sessions[slot].state.output_rtt_ms = out_stats.msRTT;
        g_sessions[slot].state.output_rtt_valid = out_stats.msRTT > 0.0;
        g_sessions[slot].state.output_sent_packets_total = out_stats.pktSentTotal;
        g_sessions[slot].state.output_send_loss_total = out_stats.pktSndLossTotal;
        g_sessions[slot].state.output_send_drop_total = out_stats.pktSndDropTotal;
        g_sessions[slot].state.output_retrans_total = out_stats.pktRetransTotal;
    }
    g_sessions[slot].state.leg_count = leg_count;
    for (int li = 0; li < leg_count; ++li) {
        g_sessions[slot].state.legs[li] = legs[li];
    }
    g_sessions[slot].state.last_stats_at_ms = now;
    pthread_mutex_unlock(&g_sessions[slot].state_mu);
}

static int stream_input_still_connected(SRTSOCKET conn) {
    SRT_SOCKSTATUS st = srt_getsockstate(conn);
    return st == SRTS_CONNECTED || st == SRTS_LISTENING || st == SRTS_CONNECTING;
}

static void write_status_response(int client_fd) {
    relay_stream_state_t states[MAX_ACTIVE_SESSIONS];
    char last_error[sizeof g_last_error];
    long long updated_at_ms = now_ms();

    /* Copy each slot under its own lock: the snapshot is consistent per
     * stream (which is all the status API promises), and the copy loop no
     * longer stalls every session's per-packet bookkeeping behind one
     * process-wide lock. */
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        pthread_mutex_lock(&g_sessions[i].state_mu);
        states[i] = g_sessions[i].state;
        pthread_mutex_unlock(&g_sessions[i].state_mu);
    }
    pthread_mutex_lock(&g_last_error_mu);
    memcpy(last_error, g_last_error, sizeof last_error);
    pthread_mutex_unlock(&g_last_error_mu);

    int out_fd = dup(client_fd);
    if (out_fd < 0) return;
    FILE *f = fdopen(out_fd, "w");
    if (!f) {
        close(out_fd);
        return;
    }

    fprintf(f,
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n"
            "\r\n"
            "{\n"
            "  \"pid\": %ld,\n"
            "  \"startedAtMs\": %lld,\n"
            "  \"updatedAtMs\": %lld,\n"
            "  \"lastError\": ",
            (long)getpid(), g_started_at_ms, updated_at_ms);
    if (last_error[0]) {
        fputc('"', f);
        json_write_escaped(f, last_error);
        fputs("\",\n", f);
    } else {
        fputs("null,\n", f);
    }
    fputs("  \"activeStreamIds\": [", f);

    int first = 1;
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (!states[i].streamid[0] || !states[i].input_active) continue;
        fprintf(f, "%s\n    \"", first ? "\n" : ",\n");
        json_write_escaped(f, states[i].streamid);
        fputc('"', f);
        first = 0;
    }
    fprintf(f, "%s\n  ],\n  \"streamStates\": [", first ? "" : "\n");

    first = 1;
    for (int i = 0; i < MAX_ACTIVE_SESSIONS; ++i) {
        if (!states[i].streamid[0]) continue;
        fprintf(f, "%s\n    {\n      \"streamId\": \"", first ? "\n" : ",\n");
        json_write_escaped(f, states[i].streamid);
        fprintf(f,
                "\",\n      \"inputActive\": %s,\n      \"outputConnected\": %s,\n      "
                "\"retryFailures\": %d,\n      \"forwardedPackets\": %lld,\n      "
                "\"forwardedBytes\": %lld,\n      \"lastPacketAt\": %lld,\n      "
                "\"lastInputPacketAt\": %lld,\n      \"recvPacketsTotal\": ",
                states[i].input_active ? "true" : "false",
                states[i].output_connected ? "true" : "false", states[i].retry_failures,
                states[i].forwarded_packets, states[i].forwarded_bytes, states[i].last_packet_at_ms,
                states[i].last_input_packet_at_ms);
        if (states[i].recv_packets_total_valid) {
            fprintf(f, "%lld", states[i].recv_packets_total);
        } else {
            fputs("null", f);
        }
        fprintf(f,
                ",\n      \"recvUniquePacketsTotal\": %lld,\n      \"recvLossTotal\": %d,\n      "
                "\"recvDropTotal\": %d,\n      \"retransTotal\": %d,\n      \"inputRttMs\": ",
                states[i].recv_unique_packets_total, states[i].recv_loss_total,
                states[i].recv_drop_total, states[i].retrans_total);
        if (states[i].input_rtt_valid) {
            fprintf(f, "%.3f", states[i].input_rtt_ms);
        } else {
            fputs("null", f);
        }
        fputs(",\n      \"outputRttMs\": ", f);
        if (states[i].output_rtt_valid) {
            fprintf(f, "%.3f", states[i].output_rtt_ms);
        } else {
            fputs("null", f);
        }
        fprintf(f,
                ",\n      \"outputSentPacketsTotal\": %lld,\n      \"outputSendLossTotal\": %d,\n"
                "      \"outputSendDropTotal\": %d,\n      \"outputRetransTotal\": %d,\n      "
                "\"legs\": [",
                states[i].output_sent_packets_total, states[i].output_send_loss_total,
                states[i].output_send_drop_total, states[i].output_retrans_total);

        int leg_first = 1;
        for (int j = 0; j < states[i].leg_count; ++j) {
            const relay_leg_state_t *leg = &states[i].legs[j];
            fprintf(f, "%s\n        {\n          \"ip\": \"", leg_first ? "\n" : ",\n");
            json_write_escaped(f, leg->ip);
            fprintf(f,
                    "\",\n          \"port\": %d,\n          \"state\": \"%s\",\n          "
                    "\"rttMs\": ",
                    leg->port, member_state_str(leg->member_state));
            if (leg->rtt_valid) {
                fprintf(f, "%.3f", leg->rtt_ms);
            } else {
                fputs("null", f);
            }
            fputs(",\n          \"recvPacketsTotal\": ", f);
            if (leg->stats_valid) {
                fprintf(f,
                        "%lld,\n          \"recvUniquePacketsTotal\": %lld,\n          "
                        "\"recvLossTotal\": %d,\n          \"recvDropTotal\": %d,\n          "
                        "\"retransTotal\": %d\n        }",
                        leg->recv_packets_total, leg->recv_unique_packets_total,
                        leg->recv_loss_total, leg->recv_drop_total, leg->retrans_total);
            } else {
                fputs("null,\n          \"recvUniquePacketsTotal\": null,\n          "
                      "\"recvLossTotal\": null,\n          \"recvDropTotal\": null,\n          "
                      "\"retransTotal\": null\n        }",
                      f);
            }
            leg_first = 0;
        }
        fprintf(f, "%s\n      ],\n      \"lastErrorAt\": %lld,\n      \"lastError\": ",
                leg_first ? "" : "\n", states[i].last_error_at_ms);
        if (states[i].last_error[0]) {
            fputc('"', f);
            json_write_escaped(f, states[i].last_error);
            fputs("\"\n    }", f);
        } else {
            fputs("null\n    }", f);
        }
        first = 0;
    }
    fprintf(f, "%s\n  ]\n}\n", first ? "" : "\n");
    fclose(f);
}

static void *status_http_main(void *arg) {
    (void)arg;

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) {
        set_last_errorf("status socket(): %s", strerror(errno));
        perror("status socket");
        return NULL;
    }

    int one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = htons((unsigned short)g_status_port);

    if (bind(srv, (struct sockaddr *)&sa, sizeof sa) < 0) {
        set_last_errorf("status bind :%d failed: %s", g_status_port, strerror(errno));
        fprintf(stderr, "status bind :%d failed: %s\n", g_status_port, strerror(errno));
        close(srv);
        return NULL;
    }
    if (listen(srv, 16) < 0) {
        set_last_errorf("status listen failed: %s", strerror(errno));
        fprintf(stderr, "status listen failed: %s\n", strerror(errno));
        close(srv);
        return NULL;
    }

    fprintf(stderr, "Status HTTP listening on 127.0.0.1:%d\n", g_status_port);

    while (g_running) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(srv, &rfds);
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        int ready = select(srv + 1, &rfds, NULL, NULL, &tv);
        if (ready <= 0) continue;

        int client = accept(srv, NULL, NULL);
        if (client < 0) continue;

        /* Bound status I/O so a client that connects but never sends a
         * request (or stops reading the response) cannot block this thread
         * forever - that would kill the status endpoint and hang the
         * pthread_join() on shutdown. */
        struct timeval io_timeout;
        io_timeout.tv_sec = 2;
        io_timeout.tv_usec = 0;
        setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &io_timeout, sizeof io_timeout);
        setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &io_timeout, sizeof io_timeout);

        char req[1024];
        ssize_t req_len = recv(client, req, sizeof req, 0);
        if (req_len <= 0) {
            close(client);
            continue;
        }
        write_status_response(client);
        close(client);
    }

    close(srv);
    return NULL;
}

/* Sets one SRT socket option and logs on failure. The option value is
 * intentionally not logged: it may be a passphrase. */
static int set_srt_flag_checked(SRTSOCKET sock, SRT_SOCKOPT opt, const char *name, const void *val,
                                int len) {
    if (srt_setsockflag(sock, opt, val, len) == SRT_ERROR) {
        set_last_errorf("srt_setsockflag(%s): %s", name, srt_getlasterror_str());
        fprintf(stderr, "srt_setsockflag(%s): %s\n", name, srt_getlasterror_str());
        return -1;
    }
    return 0;
}

/* Returns 0 only if every option present in the query was applied. A
 * silently ignored failure here is dangerous: a rejected SRTO_PASSPHRASE
 * would leave the socket running unencrypted. */
static int apply_srt_opts(SRTSOCKET sock, const char *query) {
    char val[512];
    int rc = 0;

    if (get_param(query, "transtype", val, sizeof val)) {
        SRT_TRANSTYPE tt = strcmp(val, "live") == 0 ? SRTT_LIVE : SRTT_FILE;
        rc |= set_srt_flag_checked(sock, SRTO_TRANSTYPE, "transtype", &tt, sizeof tt);
    }
    if (get_param(query, "groupconnect", val, sizeof val)) {
        int v = atoi(val);
        rc |= set_srt_flag_checked(sock, SRTO_GROUPCONNECT, "groupconnect", &v, sizeof v);
    }
    if (get_param(query, "latency", val, sizeof val)) {
        int v = atoi(val);
        rc |= set_srt_flag_checked(sock, SRTO_LATENCY, "latency", &v, sizeof v);
    }
    if (get_param(query, "rcvlatency", val, sizeof val)) {
        int v = atoi(val);
        rc |= set_srt_flag_checked(sock, SRTO_RCVLATENCY, "rcvlatency", &v, sizeof v);
    }
    if (get_param(query, "passphrase", val, sizeof val)) {
        rc |= set_srt_flag_checked(sock, SRTO_PASSPHRASE, "passphrase", val, (int)strlen(val));
    }
    if (get_param(query, "pbkeylen", val, sizeof val)) {
        int v = atoi(val);
        rc |= set_srt_flag_checked(sock, SRTO_PBKEYLEN, "pbkeylen", &v, sizeof v);
    }
    return rc;
}

/* SRT only accepts passphrases of 10..79 characters; anything else makes
 * srt_setsockflag(SRTO_PASSPHRASE) fail. Validate up front so a bad value
 * is a startup error instead of a mysteriously rejected (input) or
 * silently unencrypted (output) connection. */
static int query_passphrase_ok(const char *query, const char *which) {
    char val[512];
    if (!get_param(query, "passphrase", val, sizeof val) || !val[0]) return 1;
    size_t len = strlen(val);
    if (len < 10 || len > 79) {
        fprintf(stderr, "%s passphrase must be 10..79 characters, got %zu\n", which, len);
        return 0;
    }
    return 1;
}

static int get_streamid(SRTSOCKET sock, char *sid, size_t sid_sz) {
    if (sid_sz == 0) return 0;
    sid[0] = '\0';
    int sid_len = (int)sid_sz - 1;
    if (srt_getsockflag(sock, SRTO_STREAMID, sid, &sid_len) == SRT_ERROR) return 0;
    if (sid_len < 0) sid_len = 0;
    if ((size_t)sid_len >= sid_sz) sid_len = (int)sid_sz - 1;
    sid[sid_len] = '\0';
    return sid_len > 0;
}

static SRTSOCKET connect_srt_output(const relay_config_t *cfg, const char *streamid,
                                    int tracker_slot) {
    SRTSOCKET srt_out = srt_create_socket();
    if (srt_out == SRT_INVALID_SOCK) {
        set_last_errorf("srt_create_socket(out): %s", srt_getlasterror_str());
        fprintf(stderr, "srt_create_socket(out): %s\n", srt_getlasterror_str());
        return SRT_INVALID_SOCK;
    }
    set_tracked_session_output_socket(tracker_slot, srt_out);

    SRT_TRANSTYPE tt = SRTT_LIVE;
    int opt_rc = set_srt_flag_checked(srt_out, SRTO_TRANSTYPE, "transtype", &tt, sizeof tt);
    opt_rc |= apply_srt_opts(srt_out, cfg->out_query);

    if (streamid && streamid[0]) {
        opt_rc |= set_srt_flag_checked(srt_out, SRTO_STREAMID, "streamid", streamid,
                                       (int)strlen(streamid));
    }

    int cto = 3000;
    opt_rc |= set_srt_flag_checked(srt_out, SRTO_CONNTIMEO, "conntimeo", &cto, sizeof cto);

    /* Never connect with a partially applied option set: if the passphrase
     * (or any other option) was rejected, the connection could go out
     * unencrypted or misrouted. Treat it as a failed attempt and let the
     * caller's retry/backoff handle it. */
    if (opt_rc != 0) {
        if (take_tracked_output_socket(tracker_slot, srt_out) != SRT_INVALID_SOCK) {
            srt_close(srt_out);
        }
        return SRT_INVALID_SOCK;
    }

    if (srt_connect(srt_out, (struct sockaddr *)&cfg->out_addr, (int)cfg->out_addrlen) ==
        SRT_ERROR) {
        set_last_errorf("srt_connect: %s", srt_getlasterror_str());
        fprintf(stderr, "srt_connect: %s\n", srt_getlasterror_str());
        if (take_tracked_output_socket(tracker_slot, srt_out) != SRT_INVALID_SOCK) {
            srt_close(srt_out);
        }
        return SRT_INVALID_SOCK;
    }
    if (!tracked_socket_is_current(tracker_slot, SRT_INVALID_SOCK, srt_out)) {
        return SRT_INVALID_SOCK;
    }

    return srt_out;
}

static SRTSOCKET connect_srt_output_with_retry(const relay_config_t *cfg, const char *streamid,
                                               int state_slot, int tracker_slot,
                                               long long *next_retry_at_ms) {
    SRTSOCKET srt_out = connect_srt_output(cfg, streamid, tracker_slot);
    if (srt_out != SRT_INVALID_SOCK) {
        set_stream_output_connected(state_slot, 1);
        if (next_retry_at_ms) *next_retry_at_ms = 0;
        return srt_out;
    }

    increment_stream_retry_failures(state_slot);
    int failures = get_stream_retry_failures(state_slot);
    int delay_ms = get_retry_delay_ms(failures);
    set_stream_output_connected(state_slot, 0);
    set_stream_errorf(state_slot, "Failed to publish to downstream output; retrying in %d ms",
                      delay_ms);
    if (next_retry_at_ms) *next_retry_at_ms = now_mono_ms() + delay_ms;
    return SRT_INVALID_SOCK;
}

static void *session_main(void *arg) {
    session_args_t *args = (session_args_t *)arg;
    SRTSOCKET conn = args->conn;
    const relay_config_t *cfg = args->cfg;
    int tracker_slot = args->tracker_slot;
    free(args);

    char streamid[1024];
    if (!get_streamid(conn, streamid, sizeof streamid)) {
        if (!get_param(cfg->out_query, "streamid", streamid, sizeof streamid)) {
            streamid[0] = '\0';
        }
    }
    set_tracked_session_streamid(tracker_slot, streamid);

    fprintf(stderr, "Accepted bonded SRT source streamid=%s\n", streamid[0] ? streamid : "(empty)");
    int state_slot = claim_stream_state(streamid, tracker_slot);
    SRTSOCKET srt_out = SRT_INVALID_SOCK;
    long long next_output_retry_at_ms = 0;

    if (state_slot == -2) {
        long long deadline = now_mono_ms() + DUPLICATE_TAKEOVER_WAIT_MS;
        int takeover_started = 0;
        do {
            long long now = now_mono_ms();
            long long input_age_ms = 0;
            int stream_present = stream_input_age_ms(streamid, now, &input_age_ms) == 0;

            if (!stream_present) {
                state_slot = claim_stream_state(streamid, tracker_slot);
                if (state_slot != -2) break;
            } else if (input_age_ms >= DUPLICATE_TAKEOVER_STALE_MS) {
                if (!takeover_started) {
                    set_last_errorf("Duplicate publisher takeover for stale streamid=%s", streamid);
                    fprintf(stderr, "Duplicate publisher takeover for stale streamid=%s\n",
                            streamid);
                    close_tracked_sessions_for_streamid(streamid, tracker_slot);
                    takeover_started = 1;
                }
                state_slot = claim_stream_state(streamid, tracker_slot);
                if (state_slot != -2) break;
            }

            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 50 * 1000 * 1000;
            nanosleep(&ts, NULL);
        } while (g_running && state_slot == -2 && now_mono_ms() < deadline);

        if (state_slot == -2) {
            set_last_errorf("Duplicate publisher rejected for active streamid=%s", streamid);
            fprintf(stderr, "Duplicate publisher rejected for active streamid=%s\n", streamid);
            goto cleanup;
        }
    }
    if (state_slot < 0 && streamid[0]) {
        set_last_errorf("No stream state slot available for streamid=%s", streamid);
        fprintf(stderr, "No stream state slot available for streamid=%s\n", streamid);
        goto cleanup;
    }

    srt_out = connect_srt_output_with_retry(cfg, streamid, state_slot, tracker_slot,
                                            &next_output_retry_at_ms);

    char buf[CHUNK];
    while (g_running && stream_input_still_connected(conn)) {
        if (srt_out == SRT_INVALID_SOCK && now_mono_ms() >= next_output_retry_at_ms) {
            srt_out = connect_srt_output_with_retry(cfg, streamid, state_slot, tracker_slot,
                                                    &next_output_retry_at_ms);
        }

        SRT_MSGCTRL mc = srt_msgctrl_default;
        int r = srt_recvmsg2(conn, buf, CHUNK, &mc);
        if (r == SRT_ERROR) {
            int err = srt_getlasterror(NULL);
            if (err != SRT_ECONNLOST && err != SRT_ENOCONN) {
                set_last_errorf("srt_recvmsg2: %s", srt_getlasterror_str());
                set_stream_errorf(state_slot, "Input error: %s", srt_getlasterror_str());
                fprintf(stderr, "srt_recvmsg2: %s\n", srt_getlasterror_str());
            }
            break;
        }
        if (r <= 0) continue;
        record_stream_input_progress(state_slot);
        update_stream_srt_counters(state_slot, tracker_slot, conn, srt_out, 0);

        if (srt_out != SRT_INVALID_SOCK) {
            if (srt_sendmsg2(srt_out, buf, r, NULL) == SRT_ERROR) {
                set_last_errorf("srt_sendmsg2: %s", srt_getlasterror_str());
                set_stream_errorf(state_slot, "Relay output error: %s", srt_getlasterror_str());
                set_stream_output_connected(state_slot, 0);
                fprintf(stderr, "srt_sendmsg2: %s\n", srt_getlasterror_str());
                if (take_tracked_output_socket(tracker_slot, srt_out) != SRT_INVALID_SOCK) {
                    srt_close(srt_out);
                }
                srt_out = SRT_INVALID_SOCK;
                next_output_retry_at_ms = now_mono_ms();
                continue;
            }
            record_stream_forward_progress(state_slot, r);
        }
    }

    update_stream_srt_counters(state_slot, tracker_slot, conn, srt_out, 1);
    if (srt_out != SRT_INVALID_SOCK &&
        take_tracked_output_socket(tracker_slot, srt_out) != SRT_INVALID_SOCK) {
        srt_close(srt_out);
    }
    remove_stream_state(state_slot);
    fprintf(stderr, "Connection closed streamid=%s\n", streamid[0] ? streamid : "(empty)");

cleanup:
    if (take_tracked_input_socket(tracker_slot, conn) != SRT_INVALID_SOCK) {
        srt_close(conn);
    }
    unregister_session_thread(tracker_slot);
    return NULL;
}

static int resolve_ipv4_addr(const char *host, int port, int ai_flags, const char *context,
                             struct sockaddr_storage *addr, socklen_t *addrlen) {
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = ai_flags;

    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    int rc = getaddrinfo(host, portstr, &hints, &res);
    if (rc != 0 || !res) {
        fprintf(stderr, "getaddrinfo failed for %s %s:%d: %s\n", context, host, port,
                rc == 0 ? "no address" : gai_strerror(rc));
        return -1;
    }

    *addrlen = (socklen_t)res->ai_addrlen;
    memcpy(addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return 0;
}

static int resolve_output(const char *host, int port, relay_config_t *cfg) {
    return resolve_ipv4_addr(host, port, 0, "output", &cfg->out_addr, &cfg->out_addrlen);
}

static int resolve_input_bind_addr(const char *host, int port, struct sockaddr_in *sa) {
    memset(sa, 0, sizeof *sa);
    sa->sin_family = AF_INET;
    sa->sin_port = htons((unsigned short)port);

    if (!host || !host[0] || strcmp(host, "0.0.0.0") == 0 || strcmp(host, "*") == 0) {
        sa->sin_addr.s_addr = htonl(INADDR_ANY);
        return 0;
    }

    struct sockaddr_storage resolved;
    socklen_t resolved_len = 0;
    if (resolve_ipv4_addr(host, port, AI_PASSIVE, "input", &resolved, &resolved_len) < 0) {
        return -1;
    }

    (void)resolved_len;
    memcpy(sa, &resolved, sizeof *sa);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc == 2 && (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)) {
        puts(RELAY_VERSION_STRING);
        return 0;
    }

    if (argc != 2 && argc != 3) {
        fprintf(stderr,
                "Usage: %s [--version]\n"
                "   or: %s <config.json>\n"
                "   or: %s <srt-input-uri> <srt-output-uri>\n",
                argv[0], argv[0], argv[0]);
        return 1;
    }

    char input_uri[2048];
    char output_uri[2048];
    if (argc == 2) {
        file_config_t file_cfg;
        if (load_config_file(argv[1], &file_cfg) < 0) {
            fprintf(stderr, "Bad config file: %s\n", argv[1]);
            return 1;
        }
        if (build_srt_uri(input_uri, sizeof input_uri, file_cfg.input_host, file_cfg.input_port,
                          file_cfg.passphrase, 1) != 0 ||
            build_srt_uri(output_uri, sizeof output_uri, file_cfg.output_host, file_cfg.output_port,
                          file_cfg.passphrase, 0) != 0) {
            fprintf(stderr, "Bad config file: failed to build SRT URI\n");
            return 1;
        }
        if (file_cfg.status_port > 0 && file_cfg.status_port <= 65535) {
            g_status_port = file_cfg.status_port;
        }
    } else {
        strncpy(input_uri, argv[1], sizeof input_uri - 1);
        input_uri[sizeof input_uri - 1] = '\0';
        strncpy(output_uri, argv[2], sizeof output_uri - 1);
        output_uri[sizeof output_uri - 1] = '\0';
    }

    char in_scheme[8], in_host[64], in_query[1024];
    char out_scheme[8], out_host[256], out_query[1024];
    int in_port, out_port;

    if (parse_uri(input_uri, in_scheme, sizeof in_scheme, in_host, sizeof in_host, &in_port,
                  in_query, sizeof in_query) < 0) {
        fprintf(stderr, "Bad input URI: %s\n", input_uri);
        return 1;
    }
    if (parse_uri(output_uri, out_scheme, sizeof out_scheme, out_host, sizeof out_host, &out_port,
                  out_query, sizeof out_query) < 0) {
        fprintf(stderr, "Bad output URI: %s\n", output_uri);
        return 1;
    }
    if (strcmp(in_scheme, "srt") != 0) {
        fprintf(stderr, "Input URI must use srt://\n");
        return 1;
    }
    if (strcmp(out_scheme, "srt") != 0) {
        fprintf(stderr, "Output URI must use srt://\n");
        return 1;
    }
    if (!query_passphrase_ok(in_query, "input") || !query_passphrase_ok(out_query, "output")) {
        return 1;
    }

    relay_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    strncpy(cfg.out_query, out_query, sizeof cfg.out_query - 1);

    if (resolve_output(out_host, out_port, &cfg) < 0) return 1;
    const char *status_port = getenv("SRT_BONDING_STATUS_PORT");
    if (status_port && status_port[0]) {
        int port = atoi(status_port);
        if (port > 0 && port <= 65535) g_status_port = port;
    }
    g_started_at_ms = now_ms();

    if (install_signal_handlers() != 0) {
        fprintf(stderr, "install signal handlers failed: %s\n", strerror(errno));
        return 1;
    }
    init_session_tracking();

    srt_startup();

    SRTSOCKET srv = srt_create_socket();
    if (srv == SRT_INVALID_SOCK) {
        set_last_errorf("srt_create_socket: %s", srt_getlasterror_str());
        fprintf(stderr, "srt_create_socket: %s\n", srt_getlasterror_str());
        srt_cleanup();
        return 1;
    }

    if (apply_srt_opts(srv, in_query) != 0) {
        fprintf(stderr, "Failed to apply input SRT options\n");
        srt_close(srv);
        srt_cleanup();
        return 1;
    }

    char in_passphrase[512];
    int in_has_passphrase =
        get_param(in_query, "passphrase", in_passphrase, sizeof in_passphrase) && in_passphrase[0];
    if (in_has_passphrase) {
        /* SRTO_ENFORCEDENCRYPTION defaults to on, which makes SRT reject a
         * bad-passphrase handshake internally before srt_accept() ever sees
         * it - so the relay can't log the offending peer for fail2ban.
         * Disabling it lets the connection complete; SRTO_KMSTATE is then
         * checked explicitly right after accept, below, to require a secured
         * key exchange before a session is started. */
        int enforced = 0;
        if (set_srt_flag_checked(srv, SRTO_ENFORCEDENCRYPTION, "enforcedencryption", &enforced,
                                 sizeof enforced) != 0) {
            srt_close(srv);
            srt_cleanup();
            return 1;
        }
    }

    struct sockaddr_in sa;
    if (resolve_input_bind_addr(in_host, in_port, &sa) < 0) {
        srt_close(srv);
        srt_cleanup();
        return 1;
    }

    if (srt_bind(srv, (struct sockaddr *)&sa, sizeof sa) == SRT_ERROR) {
        set_last_errorf("srt_bind %s:%d: %s", in_host, in_port, srt_getlasterror_str());
        fprintf(stderr, "srt_bind %s:%d: %s\n", in_host, in_port, srt_getlasterror_str());
        srt_close(srv);
        srt_cleanup();
        return 1;
    }
    if (srt_listen(srv, LISTEN_BACKLOG) == SRT_ERROR) {
        set_last_errorf("srt_listen: %s", srt_getlasterror_str());
        fprintf(stderr, "srt_listen: %s\n", srt_getlasterror_str());
        srt_close(srv);
        srt_cleanup();
        return 1;
    }

    fprintf(stderr, "Listening on bonded SRT %s:%d (backlog=%d) -> %s\n", in_host, in_port,
            LISTEN_BACKLOG, output_uri);

    pthread_t status_tid;
    int status_thread_started = pthread_create(&status_tid, NULL, status_http_main, NULL) == 0;
    if (!status_thread_started) {
        set_last_errorf("pthread_create(status) failed");
        fprintf(stderr, "pthread_create(status) failed\n");
    }

    int ep = srt_epoll_create();
    if (ep < 0) {
        set_last_errorf("srt_epoll_create: %s", srt_getlasterror_str());
        fprintf(stderr, "srt_epoll_create: %s\n", srt_getlasterror_str());
        srt_close(srv);
        g_running = 0;
        if (status_thread_started) pthread_join(status_tid, NULL);
        srt_cleanup();
        return 1;
    }
    int ep_events = SRT_EPOLL_IN | SRT_EPOLL_ERR;
    if (srt_epoll_add_usock(ep, srv, &ep_events) == SRT_ERROR) {
        set_last_errorf("srt_epoll_add_usock: %s", srt_getlasterror_str());
        fprintf(stderr, "srt_epoll_add_usock: %s\n", srt_getlasterror_str());
        srt_epoll_release(ep);
        srt_close(srv);
        g_running = 0;
        if (status_thread_started) pthread_join(status_tid, NULL);
        srt_cleanup();
        return 1;
    }

    while (g_running) {
        SRT_EPOLL_EVENT ev[MAX_EVENTS];
        int n = srt_epoll_uwait(ep, ev, MAX_EVENTS, 1000);
        if (n <= 0) continue;

        for (int i = 0; i < n && g_running; ++i) {
            if (ev[i].fd != srv) continue;

            struct sockaddr_storage peer;
            int plen = (int)sizeof peer;
            SRTSOCKET conn = srt_accept(srv, (struct sockaddr *)&peer, &plen);
            if (conn == SRT_INVALID_SOCK) {
                set_last_errorf("srt_accept: %s", srt_getlasterror_str());
                fprintf(stderr, "srt_accept: %s\n", srt_getlasterror_str());
                continue;
            }

            if (in_has_passphrase) {
                SRT_KM_STATE kmstate = SRT_KM_S_UNSECURED;
                int kmlen = (int)sizeof kmstate;
                if (srt_getsockflag(conn, SRTO_KMSTATE, &kmstate, &kmlen) == SRT_ERROR ||
                    kmstate != SRT_KM_S_SECURED) {
                    char peer_ip[INET6_ADDRSTRLEN] = "?";
                    int peer_port = 0;
                    format_leg_addr(&peer, peer_ip, sizeof peer_ip, &peer_port);
                    set_last_errorf("Rejected connection (bad passphrase) from %s:%d", peer_ip,
                                    peer_port);
                    fprintf(stderr, "Rejected connection (bad passphrase) from %s:%d\n", peer_ip,
                            peer_port);
                    srt_close(conn);
                    continue;
                }
            }

            session_args_t *args = (session_args_t *)calloc(1, sizeof *args);
            if (!args) {
                set_last_errorf("calloc(session): %s", strerror(errno));
                fprintf(stderr, "calloc(session): %s\n", strerror(errno));
                srt_close(conn);
                continue;
            }
            args->conn = conn;
            args->cfg = &cfg;
            args->tracker_slot = register_session_thread(conn);
            if (args->tracker_slot < 0) {
                set_last_errorf("Too many active sessions; rejecting connection");
                fprintf(stderr, "Too many active sessions; rejecting connection\n");
                srt_close(conn);
                free(args);
                continue;
            }

            pthread_t tid;
            if (pthread_create(&tid, NULL, session_main, args) != 0) {
                set_last_errorf("pthread_create(session) failed");
                fprintf(stderr, "pthread_create failed\n");
                unregister_session_thread(args->tracker_slot);
                srt_close(conn);
                free(args);
                continue;
            }
            pthread_detach(tid);
        }
    }

    g_running = 0;
    srt_epoll_release(ep);
    srt_close(srv);
    close_active_session_sockets();
    wait_for_session_threads();
    if (status_thread_started) pthread_join(status_tid, NULL);
    srt_cleanup();
    return 0;
}
