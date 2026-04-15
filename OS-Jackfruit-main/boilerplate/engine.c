/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "monitor_ioctl.h"

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)
#define MAX_CONTAINERS      64

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char              id[CONTAINER_ID_LEN];
    pid_t             host_pid;
    time_t            started_at;
    container_state_t state;
    unsigned long     soft_limit_bytes;
    unsigned long     hard_limit_bytes;
    int               exit_code;
    int               exit_signal;
    int               stop_requested;
    char              log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[CHILD_COMMAND_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  nice_value;
    int  log_write_fd;
} child_config_t;

typedef struct {
    int                server_fd;
    int                monitor_fd;
    int                should_stop;
    pthread_t          logger_thread;
    bounded_buffer_t   log_buffer;
    pthread_mutex_t    metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    int               pipe_read_fd;
    char              container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

static supervisor_ctx_t *g_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                 int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1],
                               &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1],
                               &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid --nice value (expected -20..19): %s\n",
                        argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *b)
{
    int rc;
    memset(b, 0, sizeof(*b));
    rc = pthread_mutex_init(&b->mutex, NULL);
    if (rc) return rc;
    rc = pthread_cond_init(&b->not_empty, NULL);
    if (rc) { pthread_mutex_destroy(&b->mutex); return rc; }
    rc = pthread_cond_init(&b->not_full, NULL);
    if (rc) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

int bounded_buffer_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);
    if (b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }
    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;
    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == 0 && !b->shutting_down)
        pthread_cond_wait(&b->not_empty, &b->mutex);
    if (b->count == 0 && b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return 0;
    }
    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;
    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return 1;
}

void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;
    char   open_ids[MAX_CONTAINERS][CONTAINER_ID_LEN];
    int    open_fds[MAX_CONTAINERS];
    int    open_count = 0;
    int    i;

    memset(open_ids, 0, sizeof(open_ids));
    for (i = 0; i < MAX_CONTAINERS; i++) open_fds[i] = -1;

    while (bounded_buffer_pop(buf, &item)) {
        int fd = -1;
        for (i = 0; i < open_count; i++) {
            if (strncmp(open_ids[i], item.container_id,
                        CONTAINER_ID_LEN) == 0) {
                fd = open_fds[i];
                break;
            }
        }
        if (fd == -1 && open_count < MAX_CONTAINERS) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log",
                     LOG_DIR, item.container_id);
            fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                memcpy(open_ids[open_count], item.container_id,
                       CONTAINER_ID_LEN);
                open_fds[open_count] = fd;
                open_count++;
            }
        }
        if (fd >= 0 && item.length > 0) {
            ssize_t w = write(fd, item.data, item.length);
            (void)w;
            fsync(fd);
        }
    }
    for (i = 0; i < open_count; i++)
        if (open_fds[i] >= 0) close(open_fds[i]);
    return NULL;
}

void *producer_thread(void *arg)
{
    producer_arg_t   *pa  = (producer_arg_t *)arg;
    bounded_buffer_t *buf = pa->buffer;
    int               rfd = pa->pipe_read_fd;
    log_item_t        item;

    while (1) {
        ssize_t n = read(rfd, item.data, sizeof(item.data));
        if (n <= 0) break;
        item.length = (size_t)n;
        memcpy(item.container_id, pa->container_id, CONTAINER_ID_LEN);
        bounded_buffer_push(buf, &item);
    }
    close(rfd);
    free(pa);
    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    sethostname(cfg->id, strlen(cfg->id));

    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    if (chdir(cfg->rootfs) != 0) { perror("chdir"); return 1; }
    if (chroot(".") != 0)        { perror("chroot"); return 1; }
    if (chdir("/") != 0)         { perror("chdir /"); return 1; }

    mkdir("/proc", 0555);
    mount("proc", "/proc", "proc", 0, NULL);

    /* Split command string into argv */
    char cmd_copy[CHILD_COMMAND_LEN];
    char *argv_buf[64];
    int   argc2 = 0;

    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';

    char *tok = strtok(cmd_copy, " ");
    while (tok && argc2 < 63) {
        argv_buf[argc2++] = tok;
        tok = strtok(NULL, " ");
    }
    argv_buf[argc2] = NULL;

    if (argc2 == 0) return 1;

    execv(argv_buf[0], argv_buf);
    perror("execv");
    return 1;
}

int register_with_monitor(int monitor_fd, const char *container_id,
                           pid_t host_pid, unsigned long soft_limit_bytes,
                           unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid              = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id,
                             pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) return -1;
    return 0;
}

static container_record_t *find_container(supervisor_ctx_t *ctx,
                                           const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (strncmp(c->id, id, CONTAINER_ID_LEN) == 0) return c;
        c = c->next;
    }
    return NULL;
}

static container_record_t *alloc_container(const control_request_t *req)
{
    container_record_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    strncpy(c->id, req->container_id, CONTAINER_ID_LEN - 1);
    c->soft_limit_bytes = req->soft_limit_bytes;
    c->hard_limit_bytes = req->hard_limit_bytes;
    c->state            = CONTAINER_STARTING;
    c->started_at       = time(NULL);
    snprintf(c->log_path, sizeof(c->log_path),
             "%s/%s.log", LOG_DIR, c->id);
    return c;
}

static void sigchld_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;
    int   wstatus;
    pid_t pid;
    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(wstatus)) {
                    c->exit_code   = WEXITSTATUS(wstatus);
                    c->exit_signal = 0;
                    c->state       = CONTAINER_EXITED;
                } else if (WIFSIGNALED(wstatus)) {
                    c->exit_signal = WTERMSIG(wstatus);
                    c->exit_code   = 128 + c->exit_signal;
                    c->state = c->stop_requested ?
                               CONTAINER_STOPPED : CONTAINER_KILLED;
                }
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd,
                                            c->id, c->host_pid);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void shutdown_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

static void handle_start(supervisor_ctx_t *ctx,
                          const control_request_t *req,
                          control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' already exists", req->container_id);
        return;
    }

    container_record_t *rec = alloc_container(req);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Out of memory");
        return;
    }

    int pipefd[2];
    if (pipe(pipefd) != 0) {
        free(rec);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "pipe() failed: %s", strerror(errno));
        return;
    }

    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        close(pipefd[0]); close(pipefd[1]);
        free(rec);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Out of memory");
        return;
    }
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,        PATH_MAX - 1);
    strncpy(cfg->command, req->command,       CHILD_COMMAND_LEN - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        free(cfg); close(pipefd[0]); close(pipefd[1]);
        free(rec);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Out of memory");
        return;
    }
    char *stack_top = stack + STACK_SIZE;

    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack_top, clone_flags, cfg);
    if (pid < 0) {
        free(stack); free(cfg);
        close(pipefd[0]); close(pipefd[1]);
        free(rec);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "clone() failed: %s", strerror(errno));
        return;
    }

    close(pipefd[1]);

    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;
    rec->next     = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, rec->id, pid,
                               req->soft_limit_bytes, req->hard_limit_bytes);

    producer_arg_t *pa = malloc(sizeof(*pa));
    if (pa) {
        pa->pipe_read_fd = pipefd[0];
        memcpy(pa->container_id, req->container_id, CONTAINER_ID_LEN);
        pa->buffer = &ctx->log_buffer;
        pthread_t tid;
        pthread_create(&tid, NULL, producer_thread, pa);
        pthread_detach(tid);
    } else {
        close(pipefd[0]);
    }

    free(stack);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "Container '%s' started with pid %d",
             req->container_id, pid);
}

static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    char buf[4096];
    int  off = 0;
    off += snprintf(buf + off, sizeof(buf) - off,
                    "%-16s %-8s %-10s %-12s %-12s\n",
                    "ID", "PID", "STATE", "SOFT(MiB)", "HARD(MiB)");
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;
    while (c && off < (int)sizeof(buf) - 80) {
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8d %-10s %-12lu %-12lu\n",
                        c->id, c->host_pid,
                        state_to_string(c->state),
                        c->soft_limit_bytes >> 20,
                        c->hard_limit_bytes >> 20);
        c = c->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = 0;
    strncpy(resp->message, buf, sizeof(resp->message) - 1);
}

static void handle_logs(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         control_response_t *resp,
                         int client_fd)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = find_container(ctx, req->container_id);
    char log_path[PATH_MAX] = {0};
    if (c) memcpy(log_path, c->log_path, sizeof(log_path));
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!c) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "No container '%s'", req->container_id);
        return;
    }

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "--- logs for %s ---\n", req->container_id);
    ssize_t w = write(client_fd, resp, sizeof(*resp));
    (void)w;

    int fd = open(log_path, O_RDONLY);
    if (fd >= 0) {
        char tmp[4096];
        ssize_t n;
        while ((n = read(fd, tmp, sizeof(tmp))) > 0) {
            w = write(client_fd, tmp, n);
            (void)w;
        }
        close(fd);
    }
    resp->status = 1;
}

static void handle_stop(supervisor_ctx_t *ctx,
                          const control_request_t *req,
                          control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = find_container(ctx, req->container_id);
    if (!c) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "No container '%s'", req->container_id);
        return;
    }
    if (c->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' is not running (state=%s)",
                 req->container_id, state_to_string(c->state));
        return;
    }
    c->stop_requested = 1;
    pid_t pid = c->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    kill(pid, SIGTERM);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "Sent SIGTERM to '%s' (pid %d)", req->container_id, pid);
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    (void)rootfs;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    mkdir(LOG_DIR, 0755);

    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }
    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "[supervisor] /dev/container_monitor not available: %s\n",
                strerror(errno));

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ctx.server_fd, 10) < 0) {
        perror("listen"); return 1;
    }
    chmod(CONTROL_PATH, 0666);

    g_ctx = &ctx;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    signal(SIGINT,  shutdown_handler);
    signal(SIGTERM, shutdown_handler);

    if (pthread_create(&ctx.logger_thread, NULL,
                       logging_thread, &ctx.log_buffer) != 0) {
        perror("pthread_create logger");
        return 1;
    }

    fprintf(stderr, "[supervisor] Ready. Listening on %s\n", CONTROL_PATH);

    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);

    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(10000);
                continue;
            }
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        control_request_t  req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        switch (req.kind) {
        case CMD_START:
        case CMD_RUN:
            handle_start(&ctx, &req, &resp);
            break;
        case CMD_PS:
            handle_ps(&ctx, &resp);
            break;
        case CMD_LOGS:
            handle_logs(&ctx, &req, &resp, client_fd);
            break;
        case CMD_STOP:
            handle_stop(&ctx, &req, &resp);
            break;
        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Unknown command");
        }

        if (resp.status != 1) {
            ssize_t w = write(client_fd, &resp, sizeof(resp));
            (void)w;
        }
        close(client_fd);

        if (req.kind == CMD_RUN && resp.status == 0) {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *c = find_container(&ctx, req.container_id);
            pid_t pid = c ? c->host_pid : -1;
            pthread_mutex_unlock(&ctx.metadata_lock);
            if (pid > 0) waitpid(pid, NULL, 0);
        }
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");

    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGKILL);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    while (waitpid(-1, NULL, WNOHANG) > 0);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *nx = c->next;
        free(c);
        c = nx;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] Clean exit.\n");
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write"); close(fd); return 1;
    }

    control_response_t resp;
    ssize_t n = read(fd, &resp, sizeof(resp));
    if (n <= 0) { close(fd); return 0; }

    printf("%s", resp.message);
    if (resp.status < 0)
        fprintf(stderr, "Error (status %d)\n", resp.status);

    if (req->kind == CMD_LOGS && resp.status == 0) {
        char buf[4096];
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            fwrite(buf, 1, n, stdout);
    }

    close(fd);
    return (resp.status >= 0) ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }
    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);
    usage(argv[0]);
    return 1;
}
