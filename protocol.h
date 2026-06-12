#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string.h> // for memset, strcpy, etc.
#include <unistd.h> // for read, write
#include <sys/socket.h> // for socket operations
#include <signal.h>     // for SIGPIPE
#include <stdbool.h> // for bool, true, false
#include <stdio.h> // for perror
#include <stdarg.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

// --- CONFIGURATION ---

#define NAME_SERVER_PORT 8000
#define MAX_USERNAME_LEN 64
#define MAX_FILENAME_LEN 256
#define MAX_ERROR_MSG_LEN 256
#define MAX_IP_LEN 64

// --- COMMAND DEFINITIONS ---
// This is the "language" our servers speak.
typedef enum {
    // Registration commands
    CMD_REGISTER_SS = 100,
    CMD_REGISTER_CLIENT = 101,

    // Acknowledgement / Error
    CMD_ACK = 200,    // General success
    CMD_ERROR = 201,  // General failure

    // File operations
    CMD_CREATE_FILE = 300,
    CMD_DELETE_FILE = 301,
    CMD_CLEAR_FILE  = 302,

    // View/listing operations
    // Client <-> NM
    CMD_VIEW_FILES = 310,
    CMD_VIEW_FILES_RESP = 311,

    // NM <-> SS (internal listing used by NM to refresh registry)
    CMD_SS_LIST_FILES = 320,
    CMD_SS_LIST_FILES_RESP = 321,

    // READ routing and response
    // Client <-> NM
    CMD_READ_FILE = 330,
    CMD_READ_FILE_RESP = 331,
    
    // Access control operations
    CMD_ADD_ACCESS = 340,
    CMD_REM_ACCESS = 341,

    // File info query
    CMD_INFO = 350,
    CMD_INFO_RESP = 351,

    // Direct client -> SS write (session begin)
    CMD_WRITE_BEGIN = 360,
    // Direct client -> SS write (apply and finalize)
    CMD_WRITE_FILE = 361,
    CMD_WRITE_DONE = 362,

    // Undo last change (Client -> NM -> SS)
    CMD_UNDO = 370,

    // Stream file content (Client -> SS via NM routing)
    CMD_STREAM = 375,

    // User listing
    CMD_LIST_USERS = 380,
    CMD_LIST_USERS_RESP = 381,

    // Execute file contents as shell commands (on Name Server)
    CMD_EXEC = 390,

    // Checkpoint operations
    CMD_CHECKPOINT_CREATE = 400,      // Create a named checkpoint snapshot
    CMD_CHECKPOINT_VIEW = 401,        // View contents of a named checkpoint
    CMD_CHECKPOINT_REVERT = 402,      // Revert file content to a named checkpoint
    CMD_CHECKPOINT_LIST = 403,        // List all checkpoint tags for a file
    CMD_CHECKPOINT_LIST_RESP = 404    // Response containing list of checkpoint tags
    ,
    // Access request workflow
    CMD_REQUEST_ACCESS = 410,         // User requests read or write access
    CMD_LIST_REQUESTS = 411,          // Owner lists pending access requests
    CMD_LIST_REQUESTS_RESP = 412,     // Response: list of requests
    CMD_RESPOND_REQUEST = 413         // Owner approves or denies a request
        ,
        // Trash bin operations
        CMD_TRASH_LIST = 420,
        CMD_TRASH_LIST_RESP = 421,
        CMD_TRASH_RECOVER = 422,
        CMD_TRASH_EMPTY = 423
} CommandCode;

// --- UNIVERSAL ERROR CODES ---
// Centralized set of error codes to be used across NM/SS/Client
// Values intentionally mirror common HTTP semantics for familiarity.
typedef enum {
    EC_BAD_REQUEST            = 400,
    EC_FORBIDDEN              = 403,
    EC_NOT_FOUND              = 404,
    EC_CONFLICT               = 409,
    EC_LOCKED                 = 423,
    EC_INTERNAL_ERROR         = 500,
    EC_BAD_GATEWAY            = 502,
    EC_SERVICE_UNAVAILABLE    = 503
} ErrorCode;

// --- DATA STRUCTURES ---
// We use fixed-size structs for simple network communication.

// Header sent before EVERY message
typedef struct {
    CommandCode command;
    int payload_size; // Size of the struct *following* this header
} MsgHeader;

// Data for CMD_REGISTER_SS
// SS -> NM
// On initialization, SS sends its client listen port and
// a best-effort snapshot of files it currently holds.
typedef struct {
    int  client_listen_port; // The port this SS will open for clients
    char initial_files[16384]; // optional newline-separated list of filenames
} MsgRegisterSS;

// Data for CMD_REGISTER_CLIENT
// Client -> NM
// Clients provide their username plus bookkeeping info about
// their view of the network (IP/NM/SS ports) for auditing.
typedef struct {
    char username[MAX_USERNAME_LEN];
    char client_ip[MAX_IP_LEN]; // textual IP of the client (may be empty)
    int  nm_port;               // NM port the client connects to
    int  ss_port;               // client-side SS port (0 if not applicable)
} MsgRegisterClient;

// Data for CMD_ERROR
// NM -> Client or SS
typedef struct {
    int  code;                       // error code (e.g., HTTP-ish or errno-ish)
    char message[MAX_ERROR_MSG_LEN]; // human readable message
} MsgError;

// Data for CMD_CREATE_FILE
// Client -> NM
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN];
} MsgCreateFile;

// Data for CMD_DELETE_FILE
// Client -> NM -> SS
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN]; // NM will overwrite with authenticated username
} MsgDeleteFile;

// Data for CMD_CLEAR_FILE
// Client -> NM -> SS
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN]; // NM will overwrite with authenticated username
} MsgClearFile;

// Data for CMD_VIEW_FILES_RESP
// NM -> Client: newline-separated list of filenames
typedef struct {
    char file_list[16384];
} MsgViewFilesResponse;

// Data for CMD_VIEW_FILES (request with flags)
// Client -> NM
typedef struct {
    int show_all;                 // 0: only accessible files, 1: all files
    int long_list;                // 0: names only, 1: include details
    char requester[MAX_USERNAME_LEN]; // NM may ignore and use authenticated username
} MsgViewFilesRequest;

// Data for CMD_SS_LIST_FILES_RESP
// SS -> NM: newline-separated list of filenames on that SS
typedef struct {
    char files[16384];
} MsgSSFileListResponse;

// Data for CMD_READ_FILE
// Client -> NM
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN]; // who is requesting (for ACL checks on SS)
} MsgReadFile;

// Data for CMD_READ_FILE_RESP
// NM -> Client: IP/port of SS to connect to, and found flag
typedef struct {
    int found;                 // 1 if found, 0 otherwise
    char ss_ip[MAX_IP_LEN];    // IPv4 string (e.g., "127.0.0.1")
    int ss_port;               // SS client listen port
} MsgReadFileResponse;

// Data for CMD_ADD_ACCESS / CMD_REM_ACCESS
// Client -> NM -> SS
// is_writer: 0 means reader, 1 means writer
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char target[MAX_USERNAME_LEN];
    int  is_writer;
    char requester[MAX_USERNAME_LEN]; // NM will overwrite with authenticated username
} MsgAccessChange;

// Data for CMD_INFO
// Client -> NM -> SS
typedef struct {
    char filename[MAX_FILENAME_LEN];
} MsgInfoRequest;

// Data for CMD_INFO_RESP
// SS -> NM -> Client
typedef struct {
    char info[2048];
} MsgInfoResponse;

// --- CHECKPOINT MESSAGES ---
// Client -> NM -> SS (create)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char tag[128];                   // checkpoint tag (sanitized on SS)
    char requester[MAX_USERNAME_LEN];
} MsgCheckpointCreate;

// Client -> NM -> SS (view)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char tag[128];
    char requester[MAX_USERNAME_LEN];
} MsgCheckpointView;

// Client -> NM -> SS (revert)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char tag[128];
    char requester[MAX_USERNAME_LEN];
} MsgCheckpointRevert;

// Client -> NM -> SS (list)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN];
} MsgCheckpointList;

// NM -> Client (list response)
typedef struct {
    char tags[16384]; // newline-separated list of checkpoint tags
} MsgCheckpointListResponse;

// --- ACCESS REQUEST MESSAGES ---
// Client -> NM -> SS: create a request
typedef struct {
    char filename[MAX_FILENAME_LEN];   // target file
    int  want_write;                   // 0 = read, 1 = write
    char requester[MAX_USERNAME_LEN];  // NM overwrites with authenticated user
} MsgAccessRequestCreate;

// Client (owner) -> NM -> SS: list pending requests
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN]; // owner (auth at NM)
} MsgAccessRequestList;

// SS -> NM -> Client: list response (each line: requester TAB type)
typedef struct {
    char requests[16384]; // newline-separated entries: username[TAB]type (READ/WRITE)
} MsgAccessRequestListResp;

// Client (owner) -> NM -> SS: respond to a request
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char target[MAX_USERNAME_LEN];   // user whose request is being answered
    int  approve;                    // 1 = approve, 0 = deny
    int  grant_write;                // if approving: 1=writer, 0=reader (ignored if deny)
    char requester[MAX_USERNAME_LEN]; // owner (auth at NM)
} MsgAccessRequestRespond;
 
// --- TRASH BIN MESSAGES ---
// Client -> NM -> SS: list trashed files for an owner
typedef struct {
    char owner[MAX_USERNAME_LEN];
} MsgTrashList;

// SS -> NM -> Client: list response (newline-separated filenames)
typedef struct {
    char files[16384];
} MsgTrashListResp;

// Client -> NM -> SS: recover a trashed file
typedef struct {
    char owner[MAX_USERNAME_LEN];
    char filename[MAX_FILENAME_LEN];      // original filename in trash
    char newname[MAX_FILENAME_LEN];       // optional new name; empty means use original
} MsgTrashRecover;

// Client -> NM -> SS: empty trash (all or single file)
typedef struct {
    char owner[MAX_USERNAME_LEN];
    char filename[MAX_FILENAME_LEN];      // optional; empty means all
    int  all;                             // 1 = remove all, 0 = remove only filename
} MsgTrashEmpty;

// Data for CMD_WRITE_BEGIN (direct client -> SS): acquire a sentence lock only
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int  sentence_index;       // 0-based
    char requester[MAX_USERNAME_LEN]; // who is requesting (for ACL checks on SS)
} MsgWriteBegin;

// Data for CMD_WRITE_FILE (direct client -> SS)
// Replace a specific sentence (0-based index) in the file with replacement text
// NOTE: Sentence-parsing/locking is handled on SS side. For MVP we enforce a max replacement size.
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int  sentence_index;       // 0-based
    char replacement[2048];    // new sentence text (UTF-8)
    char requester[MAX_USERNAME_LEN]; // who is requesting (for ACL checks on SS)
} MsgWriteFile;

// Data for CMD_WRITE_DONE (client -> SS): release a previously acquired sentence lock
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int sentence_index;        // 0-based
    char requester[MAX_USERNAME_LEN]; // who is requesting
} MsgWriteDone;

// Data for CMD_UNDO (Client -> NM -> SS)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN];
} MsgUndoRequest;

// Data for CMD_LIST_USERS_RESP
typedef struct {
    char users[16384]; // newline-separated usernames
} MsgUsersListResponse;

// Data for CMD_STREAM (Client -> SS)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN];
} MsgStreamRequest;

// Data for CMD_EXEC (Client -> NM)
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char requester[MAX_USERNAME_LEN];
} MsgExecRequest;


// --- HELPER FUNCTIONS ---
// These ensure all data is sent/received reliably,
// handling cases where TCP sends/receives data in chunks.

/**
 * @brief Reliably sends a buffer of a specific size over a socket.
 * @param socket_fd The socket file descriptor.
 * @param buffer The data to send.
 * @param size The number of bytes to send.
 * @return true on success, false on failure.
 */
static inline bool send_all(int socket_fd, const void* buffer, size_t size) {
    const char* ptr = (const char*)buffer;
    size_t total_sent = 0;
    while (total_sent < size) {
        // Avoid process termination on EPIPE by disabling SIGPIPE for this send
        #ifdef MSG_NOSIGNAL
        ssize_t sent = send(socket_fd, ptr + total_sent, size - total_sent, MSG_NOSIGNAL);
        #else
        ssize_t sent = send(socket_fd, ptr + total_sent, size - total_sent, 0);
        #endif
        if (sent <= 0) {
            // 0 means connection closed, -1 is an error
            perror("send_all");
            return false;
        }
        total_sent += (size_t)sent;
    }
    return true;
}

/**
 * @brief Reliably receives a specific number of bytes from a socket.
 * @param socket_fd The socket file descriptor.
 * @param buffer The buffer to fill.
 * @param size The number of bytes to receive.
 * @return true on success, false on failure (e.g., connection closed).
 */
static inline bool recv_all(int socket_fd, void* buffer, size_t size) {
    char* ptr = (char*)buffer;
    size_t total_received = 0;
    while (total_received < size) {
        ssize_t received = recv(socket_fd, ptr + total_received, size - total_received, 0);
        if (received <= 0) {
            // 0 means connection closed, -1 is an error
            if (received == 0) {
                // Connection closed gracefully
                return false;
            }
            perror("recv_all");
            return false;
        }
        total_received += (size_t)received;
    }
    return true;
}

// --- LOGGING HELPERS (timestamped) ---
// Format: [YYYY-MM-DD HH:MM:SS] [COMP] message...
static inline void _get_ts(char* buf, size_t n) {
    if (!buf || n == 0) return;
    time_t now = time(NULL);
    struct tm tmv;
#if defined(_WIN32)
    localtime_s(&tmv, &now);
#else
    localtime_r(&now, &tmv);
#endif
    strftime(buf, n, "%Y-%m-%d %H:%M:%S", &tmv);
}

// Append a single line to a component-specific log file (best-effort)
static inline void _append_log_file(const char* comp, const char* line) {
    const char* fname = NULL;
    if (comp && strcmp(comp, "NM") == 0) fname = "nm.log";
    else if (comp && strcmp(comp, "SS") == 0) fname = "ss.log";
    else fname = "client.log";
    FILE* lf = fopen(fname, "a");
    if (!lf) return;
    fputs(line, lf);
    // Ensure newline termination for file readability
    size_t L = strlen(line);
    if (L == 0 || line[L-1] != '\n') fputc('\n', lf);
    fclose(lf);
}

static inline void log_info_comp(const char* comp, const char* fmt, ...) {
    char ts[24]; _get_ts(ts, sizeof(ts));
    char msg[8192];
    va_list ap; va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    // Console
    printf("[%s] [%s] %s", ts, comp ? comp : "?", msg);
    // Record
    char line[9000];
    snprintf(line, sizeof(line), "[%s] [%s] %s", ts, comp ? comp : "?", msg);
    _append_log_file(comp ? comp : "?", line);
}

static inline void log_err_comp(const char* comp, const char* fmt, ...) {
    char ts[24]; _get_ts(ts, sizeof(ts));
    char msg[8192];
    va_list ap; va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    // Console (stderr)
    fprintf(stderr, "[%s] [%s] %s", ts, comp ? comp : "?", msg);
    // Record
    char line[9000];
    snprintf(line, sizeof(line), "[%s] [%s] %s", ts, comp ? comp : "?", msg);
    _append_log_file(comp ? comp : "?", line);
}

#define LOG_INFO_C(COMP, fmt, ...) do { \
    log_info_comp(COMP, fmt, ##__VA_ARGS__); \
} while(0)

#define LOG_ERR_C(COMP, fmt, ...) do { \
    log_err_comp(COMP, fmt, ##__VA_ARGS__); \
} while(0)

#define LOG_NM(fmt, ...)      LOG_INFO_C("NM", fmt, ##__VA_ARGS__)
#define LOGE_NM(fmt, ...)     LOG_ERR_C("NM", fmt, ##__VA_ARGS__)
#define LOG_SS(fmt, ...)      LOG_INFO_C("SS", fmt, ##__VA_ARGS__)
#define LOGE_SS(fmt, ...)     LOG_ERR_C("SS", fmt, ##__VA_ARGS__)

// Color-aware client logging (auto-disables when not a TTY or NO_COLOR is set)
static inline int _term_supports_color_file(FILE* f) {
    if (getenv("NO_COLOR") != NULL) return 0;
    if (!f) return 0;
    int fd = fileno(f);
    if (fd < 0) return 0;
    if (!isatty(fd)) return 0;
    const char* term = getenv("TERM");
    if (!term || strcmp(term, "dumb") == 0 || strcmp(term, "unknown") == 0) return 0;
    return 1;
}

#define ANSI_RED     "\033[31m"
#define ANSI_GREEN   "\033[32m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_BLUE    "\033[34m"
#define ANSI_MAGENTA "\033[35m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_RESET   "\033[0m"

#undef LOG_CLIENT
#undef LOGE_CLIENT
#define LOG_CLIENT(fmt, ...)  do { \
    char _ts[24]; _get_ts(_ts, sizeof(_ts)); \
    if (_term_supports_color_file(stdout)) { \
        fprintf(stdout, "[%s] " ANSI_CYAN "[Client]" ANSI_RESET " " fmt, _ts, ##__VA_ARGS__); \
    } else { \
        fprintf(stdout, "[%s] [Client] " fmt, _ts, ##__VA_ARGS__); \
    } \
} while(0)

#define LOGE_CLIENT(fmt, ...) do { \
    char _ts[24]; _get_ts(_ts, sizeof(_ts)); \
    if (_term_supports_color_file(stderr)) { \
        fprintf(stderr, "[%s] " ANSI_RED "[Client]" ANSI_RESET " " fmt, _ts, ##__VA_ARGS__); \
    } else { \
        fprintf(stderr, "[%s] [Client] " fmt, _ts, ##__VA_ARGS__); \
    } \
} while(0)

#endif // PROTOCOL_H

