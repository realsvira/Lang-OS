#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <dirent.h>
#include <ctype.h>
#include <signal.h>

// Forward declarations for UNDO helpers (defined later)
static int undo_snapshot(const char* filename, const char* filepath);
static int undo_restore_last(const char* filename, char* errbuf, size_t errlen);

// --- Helpers for parsing and updating access lists ---
static int list_contains(const char* list, const char* user) {
    if (!user || !*user) return 0;
    char tmp[1024];
    strncpy(tmp, list ? list : "", sizeof(tmp)-1);
    tmp[sizeof(tmp)-1] = 0;
    for (char* p = tmp; *p; ++p) { if (*p==','||*p=='\n'||*p=='\r'||*p=='\t') *p=' '; }
    char* ctx = NULL; char* tok = strtok_r(tmp, " ", &ctx);
    while (tok) { if (strcmp(tok, user) == 0) return 1; tok = strtok_r(NULL, " ", &ctx); }
    return 0;
}

static void list_append(char* list, size_t cap, const char* user) {
    if (!user || !*user) return;
    size_t len = strlen(list);
    if (len == 0) {
        strncat(list, user, cap - strlen(list) - 1);
    } else {
        strncat(list, ",", cap - strlen(list) - 1);
        strncat(list, user, cap - strlen(list) - 1);
    }
}

static int list_remove(char* list, const char* user) {
    char tmp[1024]; strncpy(tmp, list ? list : "", sizeof(tmp)-1); tmp[sizeof(tmp)-1] = 0;
    char out[1024] = "";
    char* ctx = NULL; char* tok = strtok_r(tmp, ", \t\r\n", &ctx);
    int first = 1;
    int removed = 0;
    while (tok) {
        if (strcmp(tok, user) != 0) {
            if (!first) strncat(out, ",", sizeof(out)-1);
            strncat(out, tok, sizeof(out)-1);
            first = 0;
        } else {
            removed++;
        }
        tok = strtok_r(NULL, ", \t\r\n", &ctx);
    }
    strncpy(list, out, 1023);
    list[1023] = 0;
    return removed;
}

static void trim_both(char* s) {
    if (!s) return;
    // left trim
    char* p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) {
        size_t len = strlen(p);
        memmove(s, p, len + 1);
    }
    // right trim
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n-1])) { s[n-1] = 0; n--; }
}

// --- Basic filesystem helpers ---
static int ensure_dir(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }
    if (mkdir(path, 0777) == 0) return 0;
    // If parent dirs might be missing, try to create recursively for simple paths without '/'
    return -1;
}

static int is_valid_filename(const char* fn) {
    if (!fn || !*fn) return 0;
    size_t len = strlen(fn);
    if (len >= MAX_FILENAME_LEN) return 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)fn[i];
        if (c == '/' || c == '\\') return 0;
        if (!(isalnum(c) || c=='_' || c=='-' || c=='.')) return 0;
    }
    return 1;
}

int client_listen_port;
int nm_socket;

#ifndef STORAGE_ROOT
#define STORAGE_ROOT "ss_data"
#endif

// --- WRITE Sentence Lock State ---
// Simple lock table for (filename, sentence_index) protected by a mutex.
// Start simple: immediate fail if already locked; no waiting/queuing.

#define MAX_SENTENCE_LOCKS 2048

typedef struct {
    int in_use;
    char filename[MAX_FILENAME_LEN];
    int sentence_index; // 0-based
    int owner_fd;       // socket fd of owner holding the lock
} SentenceLock;

static SentenceLock g_sentence_locks[MAX_SENTENCE_LOCKS];
static pthread_mutex_t g_sentence_locks_mutex = PTHREAD_MUTEX_INITIALIZER;

static int sentence_lock_find(const char* filename, int idx) {
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use &&
            g_sentence_locks[i].sentence_index == idx &&
            strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            return i;
        }
    }
    return -1;
}
// Acquire a lock immediately if available; 0 on success, -1 if already locked or no slot
static int sentence_lock_acquire_nowait_owner(const char* filename, int idx, int owner_fd) {
    int rc = -1;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    // already locked?
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use &&
            g_sentence_locks[i].sentence_index == idx &&
            strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            pthread_mutex_unlock(&g_sentence_locks_mutex);
            return -1;
        }
    }
    // find free slot
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (!g_sentence_locks[i].in_use) {
            g_sentence_locks[i].in_use = 1;
            strncpy(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN-1);
            g_sentence_locks[i].filename[MAX_FILENAME_LEN-1] = 0;
            g_sentence_locks[i].sentence_index = idx;
            g_sentence_locks[i].owner_fd = owner_fd;
            rc = 0;
            break;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return rc;
}

static int sentence_lock_owned_by(const char* filename, int idx, int owner_fd) {
    int owned = 0;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use &&
            g_sentence_locks[i].sentence_index == idx &&
            g_sentence_locks[i].owner_fd == owner_fd &&
            strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            owned = 1; break;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return owned;
}

static void sentence_lock_release(const char* filename, int idx) {
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use &&
            g_sentence_locks[i].sentence_index == idx &&
            strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            g_sentence_locks[i].in_use = 0;
            g_sentence_locks[i].filename[0] = '\0';
            g_sentence_locks[i].sentence_index = 0;
            g_sentence_locks[i].owner_fd = -1;
            break;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
}

static void sentence_lock_release_all_for_owner(int owner_fd) {
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && g_sentence_locks[i].owner_fd == owner_fd) {
            g_sentence_locks[i].in_use = 0;
            g_sentence_locks[i].filename[0] = '\0';
            g_sentence_locks[i].sentence_index = 0;
            g_sentence_locks[i].owner_fd = -1;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
}
                    
static int file_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int ss_create_file_do(const MsgCreateFile* m, char* errbuf, size_t errlen) {
    if (ensure_dir(STORAGE_ROOT) != 0) {
        snprintf(errbuf, errlen, "Failed to create storage dir");
        return -1;
    }
    if (!is_valid_filename(m->filename)) {
        snprintf(errbuf, errlen, "Invalid filename");
        return -1;
    }
    char txt[512], meta[512];

    // Store content exactly as provided (no extra .txt)
    snprintf(txt, sizeof(txt),  STORAGE_ROOT "/%s",       m->filename);
    snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", m->filename);

    if (file_exists(txt) || file_exists(meta)) {
        snprintf(errbuf, errlen, "File already exists");
        return -1;
    }

    FILE* f = fopen(txt, "w");
    if (!f) {
        snprintf(errbuf, errlen, "Cannot create content file");
        return -1;
    }
    fclose(f);

    FILE* mf = fopen(meta, "w");
    if (!mf) {
        snprintf(errbuf, errlen, "Cannot create metadata file");
        return -1;
    }
    time_t now = time(NULL);
    fprintf(mf, "owner:%s\n", m->owner);
    fprintf(mf, "created:%lld\n", (long long)now);
    fprintf(mf, "updated:%lld\n", (long long)now);
    // Write empty lists with no trailing space after colon for stable formatting
    fprintf(mf, "readers:\n");
    fprintf(mf, "writers:\n");
    fclose(mf);

    return 0;
}

// Delete the content file and its metadata. Returns 0 on success, -1 on failure.
static int ss_delete_file_do(const char* filename) {
    if (!is_valid_filename(filename)) {
        return -1;
    }
    if (ensure_dir(STORAGE_ROOT) != 0) {
        return -1;
    }
    char txt[512], meta[512];
    snprintf(txt, sizeof(txt),  STORAGE_ROOT "/%s",       filename);
    snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);

    // Determine owner from meta to place into trash subdir
    FILE* mf = fopen(meta, "r");
    if (!mf) return -1;
    char owner[MAX_USERNAME_LEN] = {0}; char line[1024];
    while (fgets(line, sizeof(line), mf)) {
        if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); break; }
    }
    fclose(mf);
    if (!owner[0]) return -1;

    // Prepare trash directories: ss_data/.trash/<owner>
    char trash_root[512]; snprintf(trash_root, sizeof(trash_root), STORAGE_ROOT "/.trash");
    if (ensure_dir(trash_root) != 0) return -1;
    char trash_owner[512]; snprintf(trash_owner, sizeof(trash_owner), STORAGE_ROOT "/.trash/%s", owner);
    if (ensure_dir(trash_owner) != 0) return -1;

    // Build destination paths
    char dst_txt[1024], dst_meta[1024];
    snprintf(dst_txt, sizeof(dst_txt),   "%s/%s", trash_owner, filename);
    snprintf(dst_meta, sizeof(dst_meta), "%s/%s.meta", trash_owner, filename);

    // If destination files already exist in trash, remove them first
    remove(dst_txt); remove(dst_meta);

    // Move content and meta into trash (atomic rename on same filesystem)
    if (rename(txt, dst_txt) != 0) return -1;
    if (rename(meta, dst_meta) != 0) {
        // Attempt to rollback content move if meta move fails
        rename(dst_txt, txt);
        return -1;
    }
    return 0;
}

// Replace the sentence at index 'sidx' with 'repl' text in STORAGE_ROOT/filename.
// Returns 0 on success, -1 on error with errbuf filled.
static int parse_is_delim(char c) {
    return (c == '.' || c == '!' || c == '?');
}

// Count how many sentence delimiters appear in text (., !, ?)
// This approximates the number of sentences contributed by the replacement.
static int count_sentences_in_text(const char* s) {
    if (!s) return 0;
    int cnt = 0;
    for (const char* p = s; *p; ++p) {
        if (parse_is_delim(*p)) cnt++;
    }
    return cnt;
}

// Write replacement text ensuring a single space after sentence delimiters
// if the next character is not whitespace or end-of-text. This normalizes
// inputs like "line1.line2." to "line1. line2." while preserving existing
// spacing (e.g., "Hello world. Nice day!" stays the same).
static void write_with_delimiter_spaces(FILE* tf, const char* buf, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        char c = buf[i];
        fputc(c, tf);
        if (parse_is_delim(c)) {
            char next = (i + 1 < len) ? buf[i + 1] : '\0';
            if (next && next != ' ' && next != '\n' && next != '\t' && next != '\r') {
                // inject exactly one space
                fputc(' ', tf);
            }
        }
    }
}

static int ss_write_replace_sentence_impl(const char* filepath, int sidx, const char* repl, char* errbuf, size_t errlen) {
    // Read entire file
    FILE* f = fopen(filepath, "rb");
    if (!f) { snprintf(errbuf, errlen, "Open failed"); return -1; }
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); snprintf(errbuf, errlen, "Seek failed"); return -1; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); snprintf(errbuf, errlen, "Tell failed"); return -1; }
    rewind(f);
    char* data = (char*)malloc((size_t)sz + 1);
    if (!data) { fclose(f); snprintf(errbuf, errlen, "OOM"); return -1; }
    size_t rd = fread(data, 1, (size_t)sz, f);
    fclose(f);
    data[rd] = '\0';

    // Create tmp path
    char tmppath[600];
    snprintf(tmppath, sizeof(tmppath), "%s.tmp", filepath);
    FILE* tf = fopen(tmppath, "wb");
    if (!tf) { free(data); snprintf(errbuf, errlen, "Tmp open failed"); return -1; }

    // Walk sentences
    size_t i = 0, n = rd; int idx = 0; int replaced = 0; int wrote_any = 0;
    while (i < n) {
        size_t start = i;
        // advance until delimiter or EOF
        while (i < n && !parse_is_delim(data[i])) i++;
        if (i < n && parse_is_delim(data[i])) {
            i++; // include delimiter
            // include trailing whitespace
            while (i < n && (data[i] == ' ' || data[i] == '\n' || data[i] == '\t' || data[i] == '\r')) i++;
        }
        size_t end = i; // [start,end)
        if (idx == sidx) {
            // Write replacement trimmed (avoid accumulating extra spaces)
            size_t rlen = strnlen(repl, 2048);
            size_t rstart = 0; while (rstart < rlen && isspace((unsigned char)repl[rstart])) rstart++;
            size_t rend = rlen; while (rend > rstart && isspace((unsigned char)repl[rend-1])) rend--;
            // Special case: a single '.' means explicit delete — remove the sentence entirely
            if (rend == rstart + 1 && repl[rstart] == '.') {
                // Do not write this sentence or its trailing whitespace
                // The previous sentence's trailing whitespace (already written) will separate properly
                replaced = 1;
            } else {
                if (rend > rstart) { write_with_delimiter_spaces(tf, repl + rstart, rend - rstart); wrote_any = 1; }
                // preserve original trailing whitespace so next sentence stays separated
                size_t ws_start = end;
                while (ws_start > start && (data[ws_start-1] == ' ' || data[ws_start-1] == '\n' || data[ws_start-1] == '\t' || data[ws_start-1] == '\r')) ws_start--;
                if (wrote_any && end > ws_start) fwrite(data + ws_start, 1, end - ws_start, tf);
                replaced = 1;
            }
        } else {
            if (end > start) {
                // Avoid leading whitespace at beginning of file after deletions
                size_t start2 = start;
                if (!wrote_any) {
                    while (start2 < end && (data[start2] == ' ' || data[start2] == '\n' || data[start2] == '\t' || data[start2] == '\r')) start2++;
                }
                if (end > start2) { fwrite(data + start2, 1, end - start2, tf); wrote_any = 1; }
            }
        }
        idx++;
    }

    // Handle empty file case or missing last sentence (no delim)
    if (idx == 0) {
        // empty file — index must be 0 to replace
        if (sidx == 0) {
            size_t rlen = strnlen(repl, 2048);
            if (rlen > 0) write_with_delimiter_spaces(tf, repl, rlen);
            replaced = 1;
            idx = 1;
        }
    }

    fclose(tf);

    // If target is exactly the next sentence (append), write it now
    if (!replaced && sidx == idx) {
        FILE* tf2 = fopen(tmppath, "ab");
        if (!tf2) {
            remove(tmppath);
            snprintf(errbuf, errlen, "Tmp reopen failed");
            free(data);
            return -1;
        }
        // Append replacement, avoiding extra spaces: add a separator only if
        // file doesn't end with whitespace AND replacement doesn't start with whitespace
        size_t rlen = strnlen(repl, 2048);
        size_t rstart = 0; while (rstart < rlen && isspace((unsigned char)repl[rstart])) rstart++;
        size_t rend = rlen; while (rend > rstart && isspace((unsigned char)repl[rend-1])) rend--;
        if (n > 0 && rstart < rend) {
            char last = data[n - 1];
            if (!(last == ' ' || last == '\n' || last == '\t' || last == '\r')) {
                // only add a single space if replacement doesn't already start with whitespace
                if (!isspace((unsigned char)repl[rstart])) fputc(' ', tf2);
            }
        }
        if (rend > rstart) write_with_delimiter_spaces(tf2, repl + rstart, rend - rstart);
        fclose(tf2);
        replaced = 1;
        idx++;
    }

    free(data);

    if (!replaced || sidx < 0 || sidx > idx) {
        // cleanup tmp
        remove(tmppath);
        snprintf(errbuf, errlen, "Sentence index out of range");
        return -1;
    }

    // Atomically replace
    if (rename(tmppath, filepath) != 0) {
        remove(tmppath);
        snprintf(errbuf, errlen, "Rename failed");
        return -1;
    }
    return 0;
}

int ss_write_replace_sentence(const char* filename, int sidx, const char* repl, char* errbuf, size_t errlen) {
    if (!is_valid_filename(filename)) { snprintf(errbuf, errlen, "Invalid filename"); return -1; }
    if (ensure_dir(STORAGE_ROOT) != 0) { snprintf(errbuf, errlen, "Storage dir error"); return -1; }
    char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", filename);
    struct stat st;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        snprintf(errbuf, errlen, "Not found");
        return -1;
    }
    // Snapshot for UNDO before making changes (best-effort)
    (void)undo_snapshot(filename, path);
    int rc = ss_write_replace_sentence_impl(path, sidx, repl, errbuf, errlen);
    if (rc == 0) {
        // touch metadata 'updated'
        char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);
        FILE* mf = fopen(meta, "a");
        if (mf) { time_t now = time(NULL); fprintf(mf, "updated:%lld\n", (long long)now); fclose(mf); }
    }
    return rc;
}

// Insert a new sentence at index 'insert_idx' (0-based) with 'repl' text
// shifting existing sentences at and after that index to the right.
static int ss_write_insert_sentence_impl(const char* filepath, int insert_idx, const char* repl, char* errbuf, size_t errlen) {
    if (insert_idx < 0) { snprintf(errbuf, errlen, "Bad index"); return -1; }
    FILE* f = fopen(filepath, "rb");
    if (!f) { snprintf(errbuf, errlen, "Open failed"); return -1; }
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); snprintf(errbuf, errlen, "Seek failed"); return -1; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); snprintf(errbuf, errlen, "Tell failed"); return -1; }
    rewind(f);
    char* data = (char*)malloc((size_t)sz + 1);
    if (!data) { fclose(f); snprintf(errbuf, errlen, "OOM"); return -1; }
    size_t rd = fread(data, 1, (size_t)sz, f);
    fclose(f);
    data[rd] = '\0';

    char tmppath[600]; snprintf(tmppath, sizeof(tmppath), "%s.tmp", filepath);
    FILE* tf = fopen(tmppath, "wb");
    if (!tf) { free(data); snprintf(errbuf, errlen, "Tmp open failed"); return -1; }

    size_t i = 0, n = rd; int idx = 0; int inserted = 0; int wrote_any = 0;
    while (i < n) {
        size_t start = i;
        while (i < n && !parse_is_delim(data[i])) i++;
        if (i < n && parse_is_delim(data[i])) {
            i++;
            while (i < n && (data[i] == ' ' || data[i] == '\n' || data[i] == '\t' || data[i] == '\r')) i++;
        }
        size_t end = i;

        if (!inserted && idx == insert_idx - 1) {
            // write previous sentence without its trailing whitespace
            size_t ws_start = end;
            while (ws_start > start && (data[ws_start-1] == ' ' || data[ws_start-1] == '\n' || data[ws_start-1] == '\t' || data[ws_start-1] == '\r')) ws_start--;
            if (ws_start > start) fwrite(data + start, 1, ws_start - start, tf);
            // inserted sentence (trimmed), and only add one space between prev and inserted
            size_t rlen = strnlen(repl, 2048);
            size_t rstart = 0; while (rstart < rlen && isspace((unsigned char)repl[rstart])) rstart++;
            size_t rend = rlen; while (rend > rstart && isspace((unsigned char)repl[rend-1])) rend--;
            // Always emit a single separator space before inserted sentence
            fputc(' ', tf);
            if (rstart < rend) {
                write_with_delimiter_spaces(tf, repl + rstart, rend - rstart);
            }
            // original whitespace (between prev and next) now separates inserted and next
            if (end > ws_start) fwrite(data + ws_start, 1, end - ws_start, tf);
            inserted = 1;
        } else {
            // normal write-through
            if (end > start) {
                size_t start2 = start;
                if (!wrote_any) {
                    while (start2 < end && (data[start2] == ' ' || data[start2] == '\n' || data[start2] == '\t' || data[start2] == '\r')) start2++;
                }
                if (end > start2) { fwrite(data + start2, 1, end - start2, tf); wrote_any = 1; }
            }
        }
        idx++;
    }

    if (!inserted) {
        // If inserting at 0 into empty file or at end, just append/precede
        if (insert_idx == 0) {
            size_t rlen = strnlen(repl, 2048);
            size_t rstart = 0; while (rstart < rlen && isspace((unsigned char)repl[rstart])) rstart++;
            size_t rend = rlen; while (rend > rstart && isspace((unsigned char)repl[rend-1])) rend--;
            if (rend > rstart) write_with_delimiter_spaces(tf, repl + rstart, rend - rstart);
            if (n > 0) {
                // Add separator only if next content doesn't already start with whitespace
                if (!(data[0] == ' ' || data[0] == '\n' || data[0] == '\t' || data[0] == '\r')) fputc(' ', tf);
            }
            if (n > 0) fwrite(data, 1, n, tf);
            inserted = 1;
        } else {
            // Or append to end if index equals sentence count (handled here too)
            // Count sentences to see if insert_idx equals idx
            // idx currently equals sentence count
            if (insert_idx == idx) {
                // ensure separation only if needed and replacement doesn't start with whitespace
                size_t rlen = strnlen(repl, 2048);
                size_t rstart = 0; while (rstart < rlen && isspace((unsigned char)repl[rstart])) rstart++;
                size_t rend = rlen; while (rend > rstart && isspace((unsigned char)repl[rend-1])) rend--;
                if (n > 0 && rstart < rend) {
                    char last = data[n-1];
                    if (!(last == ' ' || last == '\n' || last == '\t' || last == '\r')) {
                        if (!isspace((unsigned char)repl[rstart])) fputc(' ', tf);
                    }
                }
                if (rend > rstart) write_with_delimiter_spaces(tf, repl + rstart, rend - rstart);
                inserted = 1;
            }
        }
    }

    free(data);
    fclose(tf);

    if (!inserted) { remove(tmppath); snprintf(errbuf, errlen, "Insert index out of range"); return -1; }
    if (rename(tmppath, filepath) != 0) { remove(tmppath); snprintf(errbuf, errlen, "Rename failed"); return -1; }
    return 0;
}

static int ss_write_insert_sentence(const char* filename, int insert_idx, const char* repl, char* errbuf, size_t errlen) {
    if (!is_valid_filename(filename)) { snprintf(errbuf, errlen, "Invalid filename"); return -1; }
    if (ensure_dir(STORAGE_ROOT) != 0) { snprintf(errbuf, errlen, "Storage dir error"); return -1; }
    char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", filename);
    struct stat st; if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) { snprintf(errbuf, errlen, "Not found"); return -1; }
    // Snapshot for UNDO before making changes (best-effort)
    (void)undo_snapshot(filename, path);
    int rc = ss_write_insert_sentence_impl(path, insert_idx, repl, errbuf, errlen);
    if (rc == 0) {
        char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);
        FILE* mf = fopen(meta, "a"); if (mf) { time_t now = time(NULL); fprintf(mf, "updated:%lld\n", (long long)now); fclose(mf); }
    }
    return rc;
}

// --- Lock helpers for index shifts / owner lookup ---
static int sentence_lock_find_for_owner(const char* filename, int owner_fd) {
    int res = -1;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && g_sentence_locks[i].owner_fd == owner_fd && strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            res = i; break;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return res;
}

static void sentence_lock_shift_from(const char* filename, int from_index, int delta) {
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0 && g_sentence_locks[i].sentence_index >= from_index) {
            g_sentence_locks[i].sentence_index += delta;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
}

// Check if any sentence lock exists on this file at or after a given index
static int sentence_lock_any_from(const char* filename, int from_index) {
    int found = 0;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use &&
            strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0 &&
            g_sentence_locks[i].sentence_index >= from_index) {
            found = 1; break;
        }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return found;
}

static int sentence_lock_any_for_file(const char* filename) {
    int found = 0;
    pthread_mutex_lock(&g_sentence_locks_mutex);
    for (int i = 0; i < MAX_SENTENCE_LOCKS; i++) {
        if (g_sentence_locks[i].in_use && strncmp(g_sentence_locks[i].filename, filename, MAX_FILENAME_LEN) == 0) { found = 1; break; }
    }
    pthread_mutex_unlock(&g_sentence_locks_mutex);
    return found;
}

// --- UNDO support: store snapshots under STORAGE_ROOT/.undo/<filename>/N.bak ---
static int ensure_undo_root(void) {
    char root[512]; snprintf(root, sizeof(root), STORAGE_ROOT "/.undo");
    if (ensure_dir(STORAGE_ROOT) != 0) return -1;
    return ensure_dir(root);
}

static int ensure_undo_dir_for(const char* filename, char* out_dir, size_t out_sz) {
    if (!is_valid_filename(filename)) return -1;
    if (ensure_undo_root() != 0) return -1;
    snprintf(out_dir, out_sz, STORAGE_ROOT "/.undo/%s", filename);
    if (ensure_dir(out_dir) != 0) return -1;
    return 0;
}

static long find_next_undo_index(const char* dirpath) {
    long max_idx = 0;
    DIR* d = opendir(dirpath);
    if (!d) return 1;
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        char* endptr = NULL; long v = strtol(ent->d_name, &endptr, 10);
        if (endptr && (*endptr == '\0' || strcmp(endptr, ".bak") == 0)) {
            if (v > max_idx) max_idx = v;
        }
    }
    closedir(d);
    return max_idx + 1;
}

static int undo_snapshot(const char* filename, const char* filepath) {
    char dir[512]; if (ensure_undo_dir_for(filename, dir, sizeof(dir)) != 0) return -1;
    long idx = find_next_undo_index(dir);
    char bakpath[700]; snprintf(bakpath, sizeof(bakpath), "%s/%ld.bak", dir, idx);
    FILE* src = fopen(filepath, "rb"); if (!src) return -1;
    FILE* dst = fopen(bakpath, "wb"); if (!dst) { fclose(src); return -1; }
    char buf[4096]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), src)) > 0) {
        if (fwrite(buf, 1, n, dst) != n) { fclose(src); fclose(dst); return -1; }
    }
    fclose(src); fclose(dst);
    return 0;
}

static int undo_restore_last(const char* filename, char* errbuf, size_t errlen) {
    if (!is_valid_filename(filename)) { snprintf(errbuf, errlen, "Invalid filename"); return -1; }
    char dir[512]; if (ensure_undo_dir_for(filename, dir, sizeof(dir)) != 0) { snprintf(errbuf, errlen, "No undo history"); return -1; }
    long best = -1; char bestname[512] = {0};
    DIR* d = opendir(dir); if (!d) { snprintf(errbuf, errlen, "No undo history"); return -1; }
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        char* endptr = NULL; long v = strtol(ent->d_name, &endptr, 10);
        if (endptr && (*endptr == '\0' || strcmp(endptr, ".bak") == 0)) {
            if (v > best) { best = v; strncpy(bestname, ent->d_name, sizeof(bestname)-1); }
        }
    }
    closedir(d);
    if (best < 0) { snprintf(errbuf, errlen, "No undo history"); return -1; }
    char bakpath[700]; snprintf(bakpath, sizeof(bakpath), "%s/%s", dir, bestname);
    char content[600]; snprintf(content, sizeof(content), STORAGE_ROOT "/%s", filename);
    char tmp[700]; snprintf(tmp, sizeof(tmp), "%s.tmp", content);
    FILE* src = fopen(bakpath, "rb"); if (!src) { snprintf(errbuf, errlen, "Open backup failed"); return -1; }
    FILE* dst = fopen(tmp, "wb"); if (!dst) { fclose(src); snprintf(errbuf, errlen, "Tmp open failed"); return -1; }
    char buf[4096]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), src)) > 0) {
        if (fwrite(buf, 1, n, dst) != n) { fclose(src); fclose(dst); remove(tmp); snprintf(errbuf, errlen, "Write failed"); return -1; }
    }
    fclose(src); fclose(dst);
    if (rename(tmp, content) != 0) { remove(tmp); snprintf(errbuf, errlen, "Replace failed"); return -1; }
    remove(bakpath);
    // Update metadata 'updated' while preserving 'accessed' and ACL lines
    char meta[600]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);
    // Read existing meta to preserve fields
    char owner[MAX_USERNAME_LEN]={0}; long long created=0; long long accessed_keep=0; char readers[1024]={0}; char writers[1024]={0};
    FILE* mfr = fopen(meta, "r");
    if (mfr) {
        char linebuf[2048];
        while (fgets(linebuf, sizeof(linebuf), mfr)) {
            if (strncmp(linebuf, "owner:",6)==0) { sscanf(linebuf+6, "%63s", owner); }
            else if (strncmp(linebuf, "created:",8)==0) { sscanf(linebuf+8, "%lld", &created); }
            else if (strncmp(linebuf, "accessed:",9)==0) { long long t=0; if (sscanf(linebuf+9, "%lld", &t)==1 && t>accessed_keep) accessed_keep=t; }
            else if (strncmp(linebuf, "readers:",8)==0) { strncpy(readers, linebuf+8, sizeof(readers)-1); }
            else if (strncmp(linebuf, "writers:",8)==0) { strncpy(writers, linebuf+8, sizeof(writers)-1); }
        }
        fclose(mfr);
        trim_both(readers); trim_both(writers);
    }
    FILE* mfw = fopen(meta, "w");
    if (mfw) {
        time_t now = time(NULL);
        fprintf(mfw, "owner:%s\n", owner);
        fprintf(mfw, "created:%lld\n", created);
        fprintf(mfw, "updated:%lld\n", (long long)now);
        if (accessed_keep>0) fprintf(mfw, "accessed:%lld\n", accessed_keep);
        fprintf(mfw, "readers:%s%s\n", (readers[0]?" ":""), readers);
        fprintf(mfw, "writers:%s%s\n", (writers[0]?" ":""), writers);
        fclose(mfw);
    }
    return 0;
}

// --- ACCESS REQUESTS SUPPORT ---
// Requests stored under STORAGE_ROOT/.requests/<filename>/<requester>.req with content "READ" or "WRITE"
static int ensure_requests_root(void) {
    char root[512]; snprintf(root, sizeof(root), STORAGE_ROOT "/.requests");
    if (ensure_dir(STORAGE_ROOT) != 0) return -1;
    return ensure_dir(root);
}

static int ensure_requests_dir_for(const char* filename, char* out_dir, size_t out_sz) {
    if (!is_valid_filename(filename)) return -1;
    if (ensure_requests_root() != 0) return -1;
    snprintf(out_dir, out_sz, STORAGE_ROOT "/.requests/%s", filename);
    if (ensure_dir(out_dir) != 0) return -1;
    return 0;
}

 
/**
 * @brief Thread to listen for direct Client connections (for READ, WRITE, etc.)
 */
typedef struct {
    int client_socket;
    char client_ip[INET_ADDRSTRLEN];
    int client_port;
} ClientSessionArgs;

static void send_error_to(int sock, int code, const char* msg) {
    MsgHeader h = {0};
    MsgError e = {0};
    h.command = CMD_ERROR;
    h.payload_size = sizeof(MsgError);
    e.code = code;
    strncpy(e.message, msg ? msg : "error", sizeof(e.message)-1);
    LOGE_SS("Sending ERROR to socket %d (%d): %s\n", sock, code, e.message);
    send_all(sock, &h, sizeof(h));
    send_all(sock, &e, sizeof(e));
}

static void* handle_one_client(void* arg) {
    ClientSessionArgs* a = (ClientSessionArgs*)arg;
    int client_socket = a->client_socket;
    char peer_ip[INET_ADDRSTRLEN]; strncpy(peer_ip, a->client_ip, sizeof(peer_ip)-1); peer_ip[sizeof(peer_ip)-1] = '\0';
    int peer_port = a->client_port;
    free(a);

    // Always use header-based protocol for client connections.
    {
        // Header-based loop
        while (true) {
            MsgHeader h;
            if (!recv_all(client_socket, &h, sizeof(h))) {
                break; // disconnected
            }
            LOG_SS("[CLIENT %s:%d fd=%d] Received command %d (payload=%d)\n", peer_ip, peer_port, client_socket, h.command, h.payload_size);

            switch (h.command) {
                case CMD_READ_FILE: {
                    if (h.payload_size != (int)sizeof(MsgReadFile)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain any unexpected payload
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgReadFile req;
                    if (!recv_all(client_socket, &req, sizeof(req))) {
                        break;
                    }
                    LOG_SS("[CLIENT %s:%d] READ request filename='%s' requester='%s'\n", peer_ip, peer_port, req.filename, req.requester);
                    if (!is_valid_filename(req.filename)) {
                        send_error_to(client_socket, 400, "Invalid filename");
                        continue;
                    }
                    // ACL check: requester must be owner or in readers or writers (writers imply read)
                    if (req.requester[0] == '\0') { send_error_to(client_socket, 403, "Unauthorized"); continue; }
                    char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", req.filename);
                    FILE* mf = fopen(meta, "r");
                    if (!mf) { send_error_to(client_socket, 404, "Not found"); continue; }
                    char owner[MAX_USERNAME_LEN] = {0};
                    char readers[1024] = {0}, writers[1024] = {0}; char line2[2048];
                    while (fgets(line2, sizeof(line2), mf)) {
                        if (strncmp(line2, "owner:", 6) == 0) { sscanf(line2+6, "%63s", owner); }
                        else if (strncmp(line2, "readers:", 8) == 0) { strncpy(readers, line2+8, sizeof(readers)-1); }
                        else if (strncmp(line2, "writers:", 8) == 0) { strncpy(writers, line2+8, sizeof(writers)-1); }
                    }
                    fclose(mf);
                    trim_both(readers); trim_both(writers);
              if (!( (owner[0] && strncmp(owner, req.requester, MAX_USERNAME_LEN)==0) ||
                  list_contains(readers, req.requester) ||
                  list_contains(writers, req.requester) )) {
                        send_error_to(client_socket, 403, "Unauthorized");
                        continue;
                    }
                    char path[512];
                    snprintf(path, sizeof(path), STORAGE_ROOT "/%s", req.filename);
                    struct stat st;
                    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
                        send_error_to(client_socket, 404, "Not found");
                        continue;
                    }
                    FILE* f = fopen(path, "rb");
                    if (!f) { send_error_to(client_socket, 500, "Open failed"); continue; }
                    // Announce payload size as file size
                    MsgHeader rh = { .command = CMD_ACK, .payload_size = (int)st.st_size };
                    LOG_SS("[CLIENT %s:%d] READ ACK size=%d for '%s'\n", peer_ip, peer_port, rh.payload_size, req.filename);
                    if (!send_all(client_socket, &rh, sizeof(rh))) { fclose(f); break; }
                    // Stream exact st_size bytes
                    char buf[4096]; size_t remaining = (size_t)st.st_size;
                    while (remaining > 0) {
                        size_t to_read = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                        size_t nr = fread(buf, 1, to_read, f);
                        if (nr == 0) break;
                        if (!send_all(client_socket, buf, nr)) { remaining = 0; break; }
                        remaining -= nr;
                    }
                    fclose(f);
                    // Record access time in metadata (portable alternative to atime)
                    {
                        char meta_path[512]; snprintf(meta_path, sizeof(meta_path), STORAGE_ROOT "/%s.meta", req.filename);
                        FILE* mf_acc = fopen(meta_path, "a");
                        if (mf_acc) { time_t now = time(NULL); fprintf(mf_acc, "accessed:%lld\n", (long long)now); fclose(mf_acc); }
                    }
                    LOG_SS("[CLIENT %s:%d] Completed READ for '%s'\n", peer_ip, peer_port, req.filename);
                    continue;
                }
                case CMD_WRITE_FILE: {
                    if (h.payload_size != (int)sizeof(MsgWriteFile)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgWriteFile req;
                    if (!recv_all(client_socket, &req, sizeof(req))) {
                        break;
                    }
                    LOG_SS("[CLIENT %s:%d] WRITE_FILE filename='%s' sidx=%d requester='%s'\n", peer_ip, peer_port, req.filename, req.sentence_index, req.requester);
                    if (!is_valid_filename(req.filename) || req.sentence_index < 0) {
                        send_error_to(client_socket, 400, "Invalid arguments");
                        continue;
                    }
                    // Bind WRITE_FILE to the caller's currently-held lock (index may have shifted)
                    int held_slot = sentence_lock_find_for_owner(req.filename, client_socket);
                    if (held_slot < 0) {
                        send_error_to(client_socket, 409, "No active lock for this file");
                        continue;
                    }
                    int used_idx = g_sentence_locks[held_slot].sentence_index;
                    // Compute sentence delta introduced by replacement (number of delimiters in replacement - 1)
                    // Special case: a single "." means delete sentence -> contributes 0 sentences
                    char repl_buf[2048];
                    strncpy(repl_buf, req.replacement, sizeof(repl_buf)-1);
                    repl_buf[sizeof(repl_buf)-1] = '\0';
                    trim_both(repl_buf);
                    int k = 0;
                    if (!(strlen(repl_buf) == 1 && repl_buf[0] == '.')) {
                        k = count_sentences_in_text(repl_buf);
                    } else {
                        k = 0;
                    }
                    int delta = k - 1;
                    // If shrinking and there are locks on later sentences, reject to avoid index collisions
                    if (delta < 0 && sentence_lock_any_from(req.filename, used_idx + 1)) {
                        send_error_to(client_socket, 423, "Conflicts with other sentence locks");
                        // keep the caller's lock; they can resolve and retry
                        continue;
                    }

                    // Perform sentence-level replacement under this lock
                    char err[256] = {0};
                    // Helper does the actual file rewrite
                    extern int ss_write_replace_sentence(const char* filename, int sidx, const char* repl, char* errbuf, size_t errlen);
                    int wrc = ss_write_replace_sentence(req.filename, used_idx, req.replacement, err, sizeof(err));
                    if (wrc == 0) {
                        // If sentence count changed, shift locks for later sentences accordingly
                        if (delta != 0) {
                            sentence_lock_shift_from(req.filename, used_idx + 1, delta);
                        }
                        // Return ACK; keep lock held until client sends CMD_WRITE_DONE
                        MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                        LOG_SS("[CLIENT %s:%d] WRITE_FILE ACK for '%s' used_idx=%d delta=%d\n", peer_ip, peer_port, req.filename, used_idx, delta);
                        if (!send_all(client_socket, &rh, sizeof(rh))) {
                            // client gone; proactively release to avoid dangling lock
                            sentence_lock_release(req.filename, used_idx);
                            break;
                        }
                    } else {
                        // On failure, release the lock so client can retry
                        sentence_lock_release(req.filename, used_idx);
                        LOGE_SS("[CLIENT %s:%d] WRITE_FILE failed for '%s': %s\n", peer_ip, peer_port, req.filename, err);
                        send_error_to(client_socket, 500, err[0] ? err : "WRITE failed");
                    }
                    continue;
                }
                case CMD_WRITE_BEGIN: {
                    if (h.payload_size != (int)sizeof(MsgWriteBegin)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgWriteBegin req;
                    if (!recv_all(client_socket, &req, sizeof(req))) { break; }
                    LOG_SS("[CLIENT %s:%d] WRITE_BEGIN filename='%s' sidx=%d requester='%s'\n", peer_ip, peer_port, req.filename, req.sentence_index, req.requester);
                    if (!is_valid_filename(req.filename) || req.sentence_index < 0) {
                        send_error_to(client_socket, 400, "Invalid arguments");
                        continue;
                    }
                    // ACL: owner or writer required to acquire lock
                    if (req.requester[0] == '\0') { send_error_to(client_socket, 403, "Unauthorized"); continue; }
                    char meta_b[512]; snprintf(meta_b, sizeof(meta_b), STORAGE_ROOT "/%s.meta", req.filename);
                    FILE* mf_b = fopen(meta_b, "r"); if (!mf_b) { send_error_to(client_socket, 404, "Not found"); continue; }
                    char owner_b[MAX_USERNAME_LEN] = {0}; char writers_b[1024] = {0}; char lineb[2048];
                    while (fgets(lineb, sizeof(lineb), mf_b)) {
                        if (strncmp(lineb, "owner:", 6) == 0) { sscanf(lineb+6, "%63s", owner_b); }
                        else if (strncmp(lineb, "writers:", 8) == 0) { strncpy(writers_b, lineb+8, sizeof(writers_b)-1); }
                    }
                    fclose(mf_b);
                    trim_both(writers_b);
                    if (!( (owner_b[0] && strncmp(owner_b, req.requester, MAX_USERNAME_LEN)==0) || list_contains(writers_b, req.requester) )) {
                        send_error_to(client_socket, 403, "Write access denied");
                        continue;
                    }
                    if (sentence_lock_acquire_nowait_owner(req.filename, req.sentence_index, client_socket) != 0) {
                        send_error_to(client_socket, 423, "Sentence is locked");
                        continue;
                    }
                    MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                    LOG_SS("[CLIENT %s:%d] WRITE_BEGIN ACK for '%s' sidx=%d\n", peer_ip, peer_port, req.filename, req.sentence_index);
                    send_all(client_socket, &rh, sizeof(rh));
                    continue;
                }
                case CMD_WRITE_DONE: {
                    if (h.payload_size != (int)sizeof(MsgWriteDone)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        continue;
                    }
                    MsgWriteDone done;
                    if (!recv_all(client_socket, &done, sizeof(done))) {
                        break;
                    }
                    LOG_SS("[CLIENT %s:%d] WRITE_DONE filename='%s' sidx=%d requester='%s'\n", peer_ip, peer_port, done.filename, done.sentence_index, done.requester);
                    // Release the caller's lock on this file even if index shifted
                    if (!sentence_lock_owned_by(done.filename, done.sentence_index, client_socket)) {
                        int slot = sentence_lock_find_for_owner(done.filename, client_socket);
                        if (slot < 0) { send_error_to(client_socket, 409, "Not lock owner"); continue; }
                        int cur_idx = g_sentence_locks[slot].sentence_index;
                        sentence_lock_release(done.filename, cur_idx);
                    } else {
                        sentence_lock_release(done.filename, done.sentence_index);
                    }
                    MsgHeader rh = { .command = CMD_ACK, .payload_size = 0 };
                    LOG_SS("[CLIENT %s:%d] WRITE_DONE ACK for '%s'\n", peer_ip, peer_port, done.filename);
                    send_all(client_socket, &rh, sizeof(rh));
                    continue;
                }
                case CMD_STREAM: {
                    if (h.payload_size != (int)sizeof(MsgStreamRequest)) {
                        send_error_to(client_socket, 400, "Bad payload size");
                        // Drain
                        size_t rem = h.payload_size; char drain[512];
                        while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                        // For STREAM errors, close to signal end to client
                        close(client_socket);
                        sentence_lock_release_all_for_owner(client_socket);
                        return NULL;
                    }
                    MsgStreamRequest rq;
                    if (!recv_all(client_socket, &rq, sizeof(rq))) { break; }
                    LOG_SS("[CLIENT %s:%d] STREAM request filename='%s' requester='%s'\n", peer_ip, peer_port, rq.filename, rq.requester);
                    if (!is_valid_filename(rq.filename)) { send_error_to(client_socket, 400, "Invalid filename"); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                    // ACL: owner/readers/writers can stream (writers imply read)
                    if (rq.requester[0] == '\0') { send_error_to(client_socket, 403, "Unauthorized"); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                    char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", rq.filename);
                    FILE* mf = fopen(meta, "r"); if (!mf) { send_error_to(client_socket, 404, "Not found"); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                    char owner[MAX_USERNAME_LEN] = {0}; char readers[1024] = {0}; char writers[1024] = {0}; char line[2048];
                    while (fgets(line, sizeof(line), mf)) {
                        if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                        else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                        else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                    }
                    fclose(mf);
                    trim_both(readers); trim_both(writers);
                    if (!((owner[0] && strncmp(owner, rq.requester, MAX_USERNAME_LEN)==0) || list_contains(readers, rq.requester) || list_contains(writers, rq.requester))) {
                        send_error_to(client_socket, 403, "Unauthorized");
                        close(client_socket);
                        sentence_lock_release_all_for_owner(client_socket);
                        return NULL;
                    }
                    char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", rq.filename);
                    // Get file size first
                    struct stat st;
                    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) { send_error_to(client_socket, 404, "Not found"); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                    FILE* f = fopen(path, "rb"); if (!f) { send_error_to(client_socket, 404, "Not found"); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                    
                    // Send ACK with total size so client knows how many bytes to expect
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = (int)st.st_size };
                    LOG_SS("[CLIENT %s:%d] STREAM start '%s' size=%lld\n", peer_ip, peer_port, rq.filename, (long long)st.st_size);
                    if (!send_all(client_socket, &ack, sizeof(ack))) { fclose(f); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                    
                    // Stream word-by-word with delays to simulate streaming
                    int c; int in_word = 0; char word[4096]; size_t wpos = 0;
                    while ((c = fgetc(f)) != EOF) {
                        int is_ws = (c==' '||c=='\n'||c=='\t'||c=='\r'||c=='\f'||c=='\v');
                        if (!is_ws) {
                            if (wpos + 1 < sizeof(word)) { word[wpos++] = (char)c; }
                            in_word = 1;
                        } else {
                            if (in_word) {
                                // end of a word: send the word, then the whitespace run, then delay
                                if (wpos > 0) { if (!send_all(client_socket, word, wpos)) { fclose(f); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; } }
                                // send this whitespace char and any subsequent whitespace chars as-is
                                char ws[4096]; size_t wss = 0; ws[wss++] = (char)c;
                                int c2;
                                while ((c2 = fgetc(f)) != EOF) {
                                    if (c2==' '||c2=='\n'||c2=='\t'||c2=='\r'||c2=='\f'||c2=='\v') {
                                        if (wss + 1 < sizeof(ws)) ws[wss++] = (char)c2;
                                    } else {
                                        ungetc(c2, f);
                                        break;
                                    }
                                }
                                if (wss > 0) { if (!send_all(client_socket, ws, wss)) { fclose(f); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; } }
                                usleep(100000);
                                wpos = 0; in_word = 0;
                            } else {
                                // outside a word: forward whitespace as-is
                                char ch = (char)c; if (!send_all(client_socket, &ch, 1)) { fclose(f); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                            }
                        }
                    }
                    if (in_word && wpos > 0) {
                        if (!send_all(client_socket, word, wpos)) { fclose(f); close(client_socket); sentence_lock_release_all_for_owner(client_socket); return NULL; }
                        usleep(100000);
                    }
                    fclose(f);
                    LOG_SS("[CLIENT %s:%d] STREAM end '%s'\n", peer_ip, peer_port, rq.filename);
                    // Record access time in metadata (portable alternative to atime)
                    {
                        char meta_path[512]; snprintf(meta_path, sizeof(meta_path), STORAGE_ROOT "/%s.meta", rq.filename);
                        FILE* mf_acc = fopen(meta_path, "a");
                        if (mf_acc) { time_t now = time(NULL); fprintf(mf_acc, "accessed:%lld\n", (long long)now); fclose(mf_acc); }
                    }
                    // Close the connection immediately for cleanliness
                    close(client_socket);
                    // Release any locks held by this client
                    sentence_lock_release_all_for_owner(client_socket);
                    return NULL;
                }
                default: {
                    // Drain unknown payload
                    size_t rem = h.payload_size; char drain[512];
                    while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(client_socket, drain, chunk)) break; rem -= chunk; }
                    send_error_to(client_socket, 400, "Unknown command");
                    continue;
                }
            }
        }
    }

    close(client_socket);
    // Release any locks held by this client
    sentence_lock_release_all_for_owner(client_socket);
    return NULL;
}

void* handle_client_connections(void* arg) {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    // Create listening socket for Clients
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        LOGE_SS("Client socket creation failed: %s\n", strerror(errno));
        return NULL;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        LOGE_SS("setsockopt failed: %s\n", strerror(errno));
        return NULL;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(client_listen_port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        LOGE_SS("Client socket bind failed: %s\n", strerror(errno));
        return NULL;
    }
    if (listen(server_fd, 5) < 0) {
        LOGE_SS("Client socket listen failed: %s\n", strerror(errno));
        return NULL;
    }

    LOG_SS("Storage Server now listening for CLIENTS on port %d...\n",
           client_listen_port);

    // --- Accept Loop for Clients ---
    while (true) {
        int client_socket;
        struct sockaddr_in caddr; socklen_t clen = sizeof(caddr);
        if ((client_socket = accept(server_fd, (struct sockaddr*)&caddr, &clen)) < 0) {
            LOGE_SS("Client accept failed: %s\n", strerror(errno));
            continue;
        }
        char ipbuf[INET_ADDRSTRLEN] = {0}; inet_ntop(AF_INET, &caddr.sin_addr, ipbuf, sizeof(ipbuf));
        int cport = (int)ntohs(caddr.sin_port);
        LOG_SS("Received a direct client connection from %s:%d (fd=%d)\n", ipbuf, cport, client_socket);

        // Spawn a detached thread per client to handle header-based loop or legacy read
        pthread_t tid;
        ClientSessionArgs* a = (ClientSessionArgs*)malloc(sizeof(ClientSessionArgs));
        if (!a) { close(client_socket); continue; }
        a->client_socket = client_socket;
        strncpy(a->client_ip, ipbuf, sizeof(a->client_ip)-1); a->client_ip[sizeof(a->client_ip)-1] = '\0';
        a->client_port = cport;
        if (pthread_create(&tid, NULL, handle_one_client, a) != 0) {
            LOGE_SS("pthread_create (client session) failed: %s\n", strerror(errno));
            close(client_socket);
            free(a);
            continue;
        }
        pthread_detach(tid);
    }
    return NULL;
}

static void nm_send_error(int code, const char* msg) {
    MsgHeader h = {0};
    MsgError  e = {0};
    h.command = CMD_ERROR;
    h.payload_size = sizeof(MsgError);
    e.code = code;
    strncpy(e.message, msg ? msg : "error", sizeof(e.message)-1);
    LOGE_SS("Sending ERROR to NM (%d): %s\n", code, e.message);
    send_all(nm_socket, &h, sizeof(h));
    send_all(nm_socket, &e, sizeof(e));
}

/**
 * @brief Thread to listen for commands from the Name Server
 */
void* listen_to_nm(void* arg) {
    MsgHeader header;
    
    // Loop forever, waiting for commands from the NM
    while (true) {
        if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
            LOGE_SS("Connection to Name Server lost! Exiting.\n");
            exit(EXIT_FAILURE); // If NM is down, SS can't function
        }

    LOG_SS("[NM] Received command %d (payload=%d)\n", header.command, header.payload_size);
        
        // Handle commands from NM
    switch (header.command) {
            case CMD_READ_FILE: {
                if (header.payload_size != sizeof(MsgReadFile)) { nm_send_error(400, "Bad payload size"); break; }
                MsgReadFile req; if (!recv_all(nm_socket, &req, sizeof(req))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(req.filename)) { nm_send_error(400, "Invalid filename"); break; }
                // ACL check: requester must be owner or in readers or writers
                if (req.requester[0] == '\0') { nm_send_error(403, "Unauthorized"); break; }
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", req.filename);
                FILE* mf = fopen(meta, "r"); if (!mf) { nm_send_error(404, "Not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                char readers[1024] = {0}, writers[1024] = {0}; char line2[2048];
                while (fgets(line2, sizeof(line2), mf)) {
                    if (strncmp(line2, "owner:", 6) == 0) { sscanf(line2+6, "%63s", owner); }
                    else if (strncmp(line2, "readers:", 8) == 0) { strncpy(readers, line2+8, sizeof(readers)-1); }
                    else if (strncmp(line2, "writers:", 8) == 0) { strncpy(writers, line2+8, sizeof(writers)-1); }
                }
                fclose(mf);
                trim_both(readers); trim_both(writers);
                if (!( (owner[0] && strncmp(owner, req.requester, MAX_USERNAME_LEN)==0) ||
                       list_contains(readers, req.requester) ||
                       list_contains(writers, req.requester) )) {
                    nm_send_error(403, "Unauthorized");
                    break;
                }
                char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", req.filename);
                struct stat st; if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) { nm_send_error(404, "Not found"); break; }
                FILE* f = fopen(path, "rb"); if (!f) { nm_send_error(500, "Open failed"); break; }
                MsgHeader rh = { .command = CMD_ACK, .payload_size = (int)st.st_size };
                LOG_SS("[NM] READ ACK size=%d for '%s' requester='%s'\n", rh.payload_size, req.filename, req.requester);
                if (!send_all(nm_socket, &rh, sizeof(rh))) { fclose(f); break; }
                char buf[4096]; size_t remaining = (size_t)st.st_size;
                while (remaining > 0) {
                    size_t to_read = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                    size_t nr = fread(buf, 1, to_read, f);
                    if (nr == 0) break;
                    if (!send_all(nm_socket, buf, nr)) { remaining = 0; break; }
                    remaining -= nr;
                }
                fclose(f);
                // Record access time in metadata
                {
                    char meta_path[512]; snprintf(meta_path, sizeof(meta_path), STORAGE_ROOT "/%s.meta", req.filename);
                    FILE* mf_acc = fopen(meta_path, "a");
                    if (mf_acc) { time_t now = time(NULL); fprintf(mf_acc, "accessed:%lld\n", (long long)now); fclose(mf_acc); }
                }
                break;
            }
            case CMD_CLEAR_FILE: {
                if (header.payload_size != sizeof(MsgClearFile)) { nm_send_error(400, "Bad payload size"); break; }
                MsgClearFile msg; if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(msg.filename)) { nm_send_error(400, "Invalid filename"); break; }
                // Disallow while any sentence locks are active for this file
                if (sentence_lock_any_for_file(msg.filename)) { LOGE_SS("[NM] CLEAR denied due to active lock '%s' requester='%s'\n", msg.filename, msg.requester); nm_send_error(423, "File has active write lock"); break; }
                // ACL: owner or writer may clear
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename);
                FILE* mf = fopen(meta, "r"); if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0}; char writers[1024] = {0}; char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                trim_both(writers);
                if (!((owner[0] && msg.requester[0] && strncmp(owner, msg.requester, MAX_USERNAME_LEN)==0) || list_contains(writers, msg.requester))) {
                    nm_send_error(403, "Write access denied");
                    break;
                }
                // Snapshot current content for UNDO, then truncate file to zero
                char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", msg.filename);
                struct stat st; if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) { nm_send_error(404, "File not found"); break; }
                (void)undo_snapshot(msg.filename, path);
                FILE* f = fopen(path, "wb"); if (!f) { nm_send_error(500, "Open failed"); break; }
                fclose(f);
                // Update metadata 'updated' timestamp
                mf = fopen(meta, "a"); if (mf) { fprintf(mf, "updated:%lld\n", (long long)time(NULL)); fclose(mf); }
                MsgHeader ack = (MsgHeader){ .command = CMD_ACK, .payload_size = 0 };
                LOG_SS("[NM] CLEAR ACK '%s' by '%s'\n", msg.filename, msg.requester);
                send_all(nm_socket, &ack, sizeof(ack));
                break;
            }
            case CMD_TRASH_LIST: {
                if (header.payload_size != sizeof(MsgTrashList)) { nm_send_error(400, "Bad payload size"); break; }
                MsgTrashList msg; if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                // List files in STORAGE_ROOT/.trash/<owner>
                char trash_dir[512]; snprintf(trash_dir, sizeof(trash_dir), STORAGE_ROOT "/.trash/%s", msg.owner);
                MsgTrashListResp resp; memset(&resp, 0, sizeof(resp));
                DIR* d = opendir(trash_dir);
                size_t pos = 0, cap = sizeof(resp.files);
                if (d) {
                    struct dirent* de; while ((de = readdir(d)) != NULL) {
                        const char* name = de->d_name; if (strcmp(name, ".")==0 || strcmp(name, "..")==0) continue; size_t nlen = strlen(name);
                        if (nlen > 5 && strcmp(name + nlen - 5, ".meta") == 0) continue; // skip .meta entries
                        int n = snprintf(resp.files + pos, (pos < cap ? cap - pos : 0), "%s\n", name);
                        if (n <= 0) break; if ((size_t)n >= (cap - pos)) { pos = cap - 1; break; } pos += (size_t)n;
                    }
                    closedir(d);
                }
                MsgHeader h = { .command = CMD_TRASH_LIST_RESP, .payload_size = sizeof(resp) };
                LOG_SS("[NM] TRASH_LIST respond owner='%s' bytes=%zu\n", msg.owner, sizeof(resp));
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
                break;
            }
            case CMD_TRASH_RECOVER: {
                if (header.payload_size != sizeof(MsgTrashRecover)) { nm_send_error(400, "Bad payload size"); break; }
                MsgTrashRecover msg; if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                // Owner-only: verify meta in trash if needed; but enforce via owner path isolation
                char src_txt[1024], src_meta[1024];
                snprintf(src_txt, sizeof(src_txt),  STORAGE_ROOT "/.trash/%s/%s", msg.owner, msg.filename);
                snprintf(src_meta, sizeof(src_meta), STORAGE_ROOT "/.trash/%s/%s.meta", msg.owner, msg.filename);
                // Destination (use newname if provided)
                const char* outname = (msg.newname[0] ? msg.newname : msg.filename);
                if (!is_valid_filename(outname)) { nm_send_error(400, "Invalid filename"); break; }
                char dst_txt[1024], dst_meta[1024];
                snprintf(dst_txt, sizeof(dst_txt),  STORAGE_ROOT "/%s", outname);
                snprintf(dst_meta, sizeof(dst_meta), STORAGE_ROOT "/%s.meta", outname);
                if (file_exists(dst_txt) || file_exists(dst_meta)) { nm_send_error(409, "File already exists"); break; }
                // Ensure root exists
                if (ensure_dir(STORAGE_ROOT) != 0) { nm_send_error(500, "Storage dir error"); break; }
                // Move back
                if (rename(src_txt, dst_txt) != 0) { LOGE_SS("[NM] RECOVER src missing '%s' owner='%s'\n", msg.filename, msg.owner); nm_send_error(404, "Not in trash"); break; }
                if (rename(src_meta, dst_meta) != 0) { // rollback
                    rename(dst_txt, src_txt);
                    nm_send_error(500, "Recover failed"); break;
                }
                // Update updated timestamp
                FILE* mf = fopen(dst_meta, "a"); if (mf) { fprintf(mf, "recovered:%lld\n", (long long)time(NULL)); fclose(mf);}                
                MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                LOG_SS("[NM] RECOVER ACK '%s' as '%s' owner='%s'\n", msg.filename, outname, msg.owner);
                send_all(nm_socket, &ack, sizeof(ack));
                break;
            }
            case CMD_TRASH_EMPTY: {
                if (header.payload_size != sizeof(MsgTrashEmpty)) { nm_send_error(400, "Bad payload size"); break; }
                MsgTrashEmpty msg; if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                char base[512]; snprintf(base, sizeof(base), STORAGE_ROOT "/.trash/%s", msg.owner);
                int errs = 0; int acted = 0;
                if (msg.all) {
                    DIR* d = opendir(base);
                    if (d) {
                        struct dirent* de; while ((de=readdir(d))!=NULL) {
                            const char* name = de->d_name; if (strcmp(name, ".")==0 || strcmp(name, "..")==0) continue;
                            size_t nlen = strlen(name); if (nlen > 5 && strcmp(name + nlen - 5, ".meta") == 0) continue;
                            char t1[1024], t2[1024]; snprintf(t1,sizeof(t1), "%s/%s", base, name); snprintf(t2,sizeof(t2), "%s/%s.meta", base, name);
                            remove(t1); remove(t2); acted=1;
                        }
                        closedir(d);
                    }
                } else if (msg.filename[0]) {
                    char t1[1024], t2[1024]; snprintf(t1,sizeof(t1), "%s/%s", base, msg.filename); snprintf(t2,sizeof(t2), "%s/%s.meta", base, msg.filename);
                    if (remove(t1)!=0) errs++; if (remove(t2)!=0) errs++; acted=1;
                }
                if (!acted) { LOGE_SS("[NM] EMPTYTRASH no items for owner='%s'\n", msg.owner); nm_send_error(404, "Nothing to remove"); break; }
                if (errs) { LOGE_SS("[NM] EMPTYTRASH partial failure for owner='%s'\n", msg.owner); nm_send_error(500, "Some items could not be removed"); break; }
                MsgHeader ack = (MsgHeader){ .command = CMD_ACK, .payload_size = 0 };
                LOG_SS("[NM] EMPTYTRASH ACK owner='%s' all=%d file='%s'\n", msg.owner, msg.all, msg.filename);
                send_all(nm_socket, &ack, sizeof(ack));
                break;
            }
            case CMD_CREATE_FILE: {
                if (header.payload_size != sizeof(MsgCreateFile)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgCreateFile msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) {
                    nm_send_error(400, "Payload read failed");
                    break;
                }
                char err[256] = {0};
                int rc = ss_create_file_do(&msg, err, sizeof(err));
                if (rc == 0) {
                    LOG_SS("Created file '%s' for owner '%s'\n", msg.filename, msg.owner);
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                    LOG_SS("[NM] CREATE ACK '%s' owner='%s'\n", msg.filename, msg.owner);
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    LOGE_SS("CREATE failed for '%s': %s\n", msg.filename, err);
                    nm_send_error(409, err[0] ? err : "CREATE failed");
                }
                break;
            }
            case CMD_SS_LIST_FILES: {
                // Drain any unexpected payload
                if (header.payload_size > 0) {
                    char drain[512];
                    size_t remaining = header.payload_size;
                    while (remaining > 0) {
                        size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                        if (!recv_all(nm_socket, drain, chunk)) break;
                        remaining -= chunk;
                    }
                }

                MsgSSFileListResponse resp = {0};
                size_t pos = 0, cap = sizeof(resp.files);

                DIR* d = opendir(STORAGE_ROOT);
                if (d) {
                    struct dirent* ent;
                    while ((ent = readdir(d)) != NULL) {
                        const char* name = ent->d_name;
                        // Skip . and ..
                        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
                        // Skip internal directories
                        if (strcmp(name, ".undo") == 0) continue;
                        if (strcmp(name, ".trash") == 0) continue;
                        // Skip metadata files
                        size_t nlen = strlen(name);
                        if (nlen > 5 && strcmp(name + nlen - 5, ".meta") == 0) continue;

                        int n = snprintf(resp.files + pos, (pos < cap ? cap - pos : 0), "%s\n", name);
                        if (n <= 0) break;
                        if ((size_t)n >= (cap - pos)) { // truncated; stop
                            pos = cap - 1;
                            break;
                        }
                        pos += (size_t)n;
                    }
                    closedir(d);
                }

                MsgHeader h = {0};
                h.command = CMD_SS_LIST_FILES_RESP;
                h.payload_size = sizeof(resp);
                LOG_SS("[NM] SS_LIST_FILES_RESP bytes=%zu\n", sizeof(resp));
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
                break;
            }
            case CMD_DELETE_FILE: {
                if (header.payload_size != sizeof(MsgDeleteFile)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgDeleteFile msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) {
                    nm_send_error(400, "Payload read failed");
                    break;
                }
                // Reject delete if any active sentence lock exists for this file
                if (sentence_lock_any_for_file(msg.filename)) {
                    nm_send_error(423, "File has active write lock");
                    break;
                }
                // Enforce owner-only delete
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename);
                FILE* mf = fopen(meta, "r");
                if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0}; char line[1024];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); break; }
                }
                fclose(mf);
                if (!(owner[0] && msg.requester[0] && strncmp(owner, msg.requester, MAX_USERNAME_LEN)==0)) {
                    nm_send_error(403, "Only owner can delete");
                    break;
                }
                int rc = ss_delete_file_do(msg.filename);
                if (rc == 0) {
                    LOG_SS("Moved file '%s' to trash for owner '%s'\n", msg.filename, owner);
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                    LOG_SS("[NM] DELETE ACK '%s' requester='%s'\n", msg.filename, msg.requester);
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    LOGE_SS("DELETE failed for '%s'\n", msg.filename);
                    nm_send_error(404, "DELETE failed");
                }
                break;
            }
            case CMD_ADD_ACCESS:
            case CMD_REM_ACCESS: {
                if (header.payload_size != sizeof(MsgAccessChange)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgAccessChange msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(msg.filename)) { nm_send_error(400, "Invalid filename"); break; }

                // Load metadata
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename);
                FILE* mf = fopen(meta, "r");
                if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                long long created=0, updated=0;
                long long accessed_keep = 0; // preserve last accessed timestamp if present
                char readers[1024] = {0}, writers[1024] = {0};
                char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "created:", 8) == 0) { sscanf(line+8, "%lld", &created); }
                    else if (strncmp(line, "updated:", 8) == 0) { sscanf(line+8, "%lld", &updated); }
                    else if (strncmp(line, "accessed:", 9) == 0) { long long t=0; if (sscanf(line+9, "%lld", &t)==1) { if (t>accessed_keep) accessed_keep=t; } }
                    else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                // Trim leading and trailing whitespace so formatting is stable
                trim_both(readers);
                trim_both(writers);

                // Owner check
                if (owner[0]==0 || strncmp(owner, msg.requester, MAX_USERNAME_LEN) != 0) {
                    nm_send_error(403, "Only owner may change access");
                    break;
                }

                if (header.command == CMD_ADD_ACCESS) {
                    if (msg.is_writer) {
                        if (list_contains(writers, msg.target)) { nm_send_error(409, "User already has writer access"); break; }
                        list_append(writers, sizeof(writers), msg.target);
                        // Ensure writers are also listed as readers
                        if (!list_contains(readers, msg.target)) {
                            list_append(readers, sizeof(readers), msg.target);
                        }
                    } else {
                        if (list_contains(readers, msg.target)) { nm_send_error(409, "User already has reader access"); break; }
                        list_append(readers, sizeof(readers), msg.target);
                    }
                } else { // REM_ACCESS: remove from BOTH readers and writers (role-agnostic)
                    int removed_w = list_remove(writers, msg.target);
                    int removed_r = list_remove(readers, msg.target);
                    if (removed_w == 0 && removed_r == 0) {
                        nm_send_error(404, "User not found");
                        break;
                    }
                }

                // Save metadata back (only on success)
                time_t now = time(NULL); updated = (long long)now;
                mf = fopen(meta, "w");
                if (!mf) { nm_send_error(500, "Failed to write meta"); break; }
                fprintf(mf, "owner:%s\n", owner);
                fprintf(mf, "created:%lld\n", created);
                fprintf(mf, "updated:%lld\n", updated);
                if (accessed_keep > 0) { fprintf(mf, "accessed:%lld\n", accessed_keep); }
                fprintf(mf, "readers:%s%s\n", (readers[0]?" ":""), readers);
                fprintf(mf, "writers:%s%s\n", (writers[0]?" ":""), writers);
                fclose(mf);

                MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                LOG_SS("[NM] %s '%s' target='%s' by '%s' ACK\n", (header.command==CMD_ADD_ACCESS?"ADD_ACCESS":"REM_ACCESS"), msg.filename, msg.target, msg.requester);
                send_all(nm_socket, &ack, sizeof(ack));
                break;
            }
            case CMD_INFO: {
                if (header.payload_size != sizeof(MsgInfoRequest)) { nm_send_error(400, "Bad payload size"); break; }
                MsgInfoRequest req; if (!recv_all(nm_socket, &req, sizeof(req))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(req.filename)) { nm_send_error(400, "Invalid filename"); break; }
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", req.filename);
                FILE* mf = fopen(meta, "r"); if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                long long created=0, updated=0;
                char readers[1024] = {0}, writers[1024] = {0};
                long long accessed_meta = 0;
                char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "created:", 8) == 0) { sscanf(line+8, "%lld", &created); }
                    else if (strncmp(line, "updated:", 8) == 0) { sscanf(line+8, "%lld", &updated); }
                    else if (strncmp(line, "accessed:", 9) == 0) { sscanf(line+9, "%lld", &accessed_meta); }
                    else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                // Trim leading and trailing whitespace
                trim_both(readers);
                trim_both(writers);
                // Gather file stats and counts
                char path[512]; snprintf(path, sizeof(path), STORAGE_ROOT "/%s", req.filename);
                struct stat st; memset(&st, 0, sizeof(st)); int have_stat = (stat(path, &st) == 0);
                long long size_bytes = have_stat ? (long long)st.st_size : 0;
                long long last_access = have_stat ? (long long)st.st_atime : 0;
                if (accessed_meta > 0 && accessed_meta > last_access) last_access = accessed_meta;
                // Count words and chars
                long long chars_cnt = 0, words_cnt = 0;
                FILE* f = fopen(path, "rb");
                if (f) {
                    int in_word = 0; int c;
                    while ((c = fgetc(f)) != EOF) {
                        chars_cnt++;
                        if (c==' '||c=='\n'||c=='\t'||c=='\r'||c=='\f'||c=='\v') { if (in_word) { words_cnt++; in_word = 0; } }
                        else { in_word = 1; }
                    }
                    if (in_word) words_cnt++;
                    fclose(f);
                }
                // Build info string
                MsgInfoResponse resp = {0};
                char created_buf[64]={0}, updated_buf[64]={0};
                char access_buf[64]={0}, mod_buf[64]={0};
                long long last_modified = have_stat ? (long long)st.st_mtime : 0;
                if (updated > 0 && updated > last_modified) last_modified = updated;
                time_t cr=(time_t)created, up=(time_t)updated, ac=(time_t)last_access, mo=(time_t)last_modified;
                struct tm tmv;
                if (localtime_r(&cr, &tmv)) strftime(created_buf, sizeof(created_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                if (localtime_r(&up, &tmv)) strftime(updated_buf, sizeof(updated_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                if (localtime_r(&ac, &tmv)) strftime(access_buf, sizeof(access_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                if (localtime_r(&mo, &tmv)) strftime(mod_buf, sizeof(mod_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                snprintf(resp.info, sizeof(resp.info),
                         "filename: %s\nowner: %s\ncreated: %s (%lld)\nupdated: %s (%lld)\nsize: %lld\nwords: %lld\nchars: %lld\nlast_access: %s (%lld)\nlast_modified: %s (%lld)\nreaders: %s\nwriters: %s\n",
                         req.filename,
                         owner[0]?owner:"",
                         created_buf[0]?created_buf:"", created,
                         updated_buf[0]?updated_buf:"", updated,
                         size_bytes,
                         words_cnt,
                         chars_cnt,
                         access_buf[0]?access_buf:"", last_access,
                         mod_buf[0]?mod_buf:"", last_modified,
                         readers[0]?readers:"",
                         writers[0]?writers:"");
                MsgHeader h = { .command = CMD_INFO_RESP, .payload_size = sizeof(resp) };
                LOG_SS("[NM] INFO RESP for '%s' bytes=%zu\n", req.filename, sizeof(resp));
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
                break;
            }
            case CMD_UNDO: {
                if (header.payload_size != sizeof(MsgUndoRequest)) { nm_send_error(400, "Bad payload size"); break; }
                MsgUndoRequest req; if (!recv_all(nm_socket, &req, sizeof(req))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(req.filename)) { nm_send_error(400, "Invalid filename"); break; }
                // ACL: owner or writer can undo
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", req.filename);
                FILE* mf = fopen(meta, "r"); if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0}; char writers[1024] = {0}; char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                trim_both(writers);
                if (!( (owner[0] && req.requester[0] && strncmp(owner, req.requester, MAX_USERNAME_LEN)==0) || list_contains(writers, req.requester) )) {
                    nm_send_error(403, "Write access denied");
                    break;
                }
                // Ensure no active sentence locks for this file
                if (sentence_lock_any_for_file(req.filename)) { nm_send_error(423, "File has active write lock"); break; }
                char err[256] = {0};
                if (undo_restore_last(req.filename, err, sizeof(err)) == 0) {
                    MsgHeader ack = (MsgHeader){ .command = CMD_ACK, .payload_size = 0 };
                    LOG_SS("[NM] UNDO ACK '%s' requester='%s'\n", req.filename, req.requester);
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    LOGE_SS("[NM] UNDO failed '%s': %s\n", req.filename, err);
                    nm_send_error(404, err[0]?err:"No undo available");
                }
                break;
            }
            // --- ACCESS REQUEST CREATE ---
            case CMD_REQUEST_ACCESS: {
                if (header.payload_size != sizeof(MsgAccessRequestCreate)) { nm_send_error(400, "Bad payload size"); break; }
                MsgAccessRequestCreate msg; if(!recv_all(nm_socket,&msg,sizeof(msg))){ nm_send_error(400,"Payload read failed"); break; }
                if(!is_valid_filename(msg.filename)){ nm_send_error(400,"Invalid filename"); break; }
                // Validate requester presence
                if(msg.requester[0]=='\0'){ nm_send_error(403,"Unauthorized"); break; }
                // Owner or writer already? Then no need to request — respond with conflict
                char meta[512]; snprintf(meta,sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; char readers[1024]={0}; char writers[1024]={0}; char line[2048];
                while(fgets(line,sizeof(line),mf)){
                    if(strncmp(line,"owner:",6)==0){ sscanf(line+6,"%63s",owner); }
                    else if(strncmp(line,"readers:",8)==0){ strncpy(readers,line+8,sizeof(readers)-1); }
                    else if(strncmp(line,"writers:",8)==0){ strncpy(writers,line+8,sizeof(writers)-1); }
                }
                fclose(mf);
                trim_both(readers); trim_both(writers);
                if( (owner[0] && strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0) || list_contains(readers,msg.requester) || list_contains(writers,msg.requester) ){
                    nm_send_error(409,"Already has access"); break; }
                // Prepare request file
                char rdir[512]; if(ensure_requests_dir_for(msg.filename,rdir,sizeof(rdir))!=0){ nm_send_error(500,"Dir error"); break; }
                char rpath[700]; snprintf(rpath,sizeof(rpath), "%s/%s.req", rdir, msg.requester);
                if(file_exists(rpath)){ nm_send_error(409,"Request exists"); break; }
                FILE* rf=fopen(rpath,"w"); if(!rf){ nm_send_error(500,"Create failed"); break; }
                fprintf(rf, "%s\n", msg.want_write?"WRITE":"READ"); fclose(rf);
                MsgHeader ack={ .command=CMD_ACK, .payload_size=0 }; LOG_SS("[NM] REQUEST_ACCESS ACK file='%s' requester='%s' write=%d\n", msg.filename, msg.requester, msg.want_write); send_all(nm_socket,&ack,sizeof(ack));
                break;
            }
            // --- ACCESS REQUEST LIST ---
            case CMD_LIST_REQUESTS: {
                if(header.payload_size != sizeof(MsgAccessRequestList)){ nm_send_error(400,"Bad payload size"); break; }
                MsgAccessRequestList msg; if(!recv_all(nm_socket,&msg,sizeof(msg))){ nm_send_error(400,"Payload read failed"); break; }
                if(!is_valid_filename(msg.filename)){ nm_send_error(400,"Invalid filename"); break; }
                // Owner auth
                char meta[512]; snprintf(meta,sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; char line[2048]; while(fgets(line,sizeof(line),mf)){ if(strncmp(line,"owner:",6)==0){ sscanf(line+6,"%63s",owner); break; } } fclose(mf);
                if(!(owner[0] && msg.requester[0] && strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0)){ nm_send_error(403,"Only owner"); break; }
                char rdir[512]; if(ensure_requests_dir_for(msg.filename,rdir,sizeof(rdir))!=0){ // empty list if no dir
                    MsgAccessRequestListResp resp={0}; MsgHeader h={ .command=CMD_LIST_REQUESTS_RESP, .payload_size=sizeof(resp)}; send_all(nm_socket,&h,sizeof(h)); send_all(nm_socket,&resp,sizeof(resp)); break; }
                DIR* d=opendir(rdir); MsgAccessRequestListResp resp={0}; size_t pos=0; if(d){ struct dirent* ent; while((ent=readdir(d))!=NULL){ if(strcmp(ent->d_name,".")==0||strcmp(ent->d_name,"..")==0) continue; size_t elen=strlen(ent->d_name); if(elen>4 && strcmp(ent->d_name+elen-4,".req")==0){ char user[MAX_USERNAME_LEN]={0}; size_t ulen=elen-4; if(ulen >= MAX_USERNAME_LEN) ulen=MAX_USERNAME_LEN-1; memcpy(user, ent->d_name, ulen); char fpath[700]; snprintf(fpath,sizeof(fpath), "%s/%s", rdir, ent->d_name); FILE* rf=fopen(fpath,"r"); if(rf){ char typ[16]={0}; if(fgets(typ,sizeof(typ),rf)){ typ[strcspn(typ,"\r\n")]=0; char linebuf[256]; snprintf(linebuf,sizeof(linebuf), "%s\t%s\n", user, strcmp(typ,"WRITE")==0?"WRITE":"READ"); size_t ln=strlen(linebuf); if(pos+ln < sizeof(resp.requests)){ memcpy(resp.requests+pos,linebuf,ln); pos+=ln; } } fclose(rf);} } }
                    closedir(d); }
                MsgHeader h={ .command=CMD_LIST_REQUESTS_RESP, .payload_size=sizeof(resp)}; LOG_SS("[NM] LIST_REQUESTS RESP file='%s' bytes=%zu\n", msg.filename, sizeof(resp)); send_all(nm_socket,&h,sizeof(h)); send_all(nm_socket,&resp,sizeof(resp));
                break;
            }
            // --- ACCESS REQUEST RESPOND ---
            case CMD_RESPOND_REQUEST: {
                if(header.payload_size != sizeof(MsgAccessRequestRespond)){ nm_send_error(400,"Bad payload size"); break; }
                MsgAccessRequestRespond msg; if(!recv_all(nm_socket,&msg,sizeof(msg))){ nm_send_error(400,"Payload read failed"); break; }
                if(!is_valid_filename(msg.filename)){ nm_send_error(400,"Invalid filename"); break; }
                // Owner auth
                char meta[512]; snprintf(meta,sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; long long created=0, updated=0; long long accessed_keep=0; char readers[1024]={0}, writers[1024]={0}; char line[2048];
                while(fgets(line,sizeof(line),mf)){
                    if(strncmp(line,"owner:",6)==0){ sscanf(line+6,"%63s",owner); }
                    else if(strncmp(line,"created:",8)==0){ sscanf(line+8,"%lld", &created); }
                    else if(strncmp(line,"updated:",8)==0){ sscanf(line+8,"%lld", &updated); }
                    else if(strncmp(line,"accessed:",9)==0){ long long t=0; if(sscanf(line+9, "%lld", &t)==1){ if(t>accessed_keep) accessed_keep=t; } }
                    else if(strncmp(line,"readers:",8)==0){ strncpy(readers,line+8,sizeof(readers)-1); }
                    else if(strncmp(line,"writers:",8)==0){ strncpy(writers,line+8,sizeof(writers)-1); }
                }
                fclose(mf); trim_both(readers); trim_both(writers);
                if(!(owner[0] && msg.requester[0] && strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0)){ nm_send_error(403,"Only owner"); break; }
                char rdir[512]; if(ensure_requests_dir_for(msg.filename,rdir,sizeof(rdir))!=0){ nm_send_error(404,"No requests" ); break; }
                char rpath[700]; snprintf(rpath,sizeof(rpath), "%s/%s.req", rdir, msg.target);
                if(!file_exists(rpath)){ nm_send_error(404,"Request not found"); break; }
                if(msg.approve){
                    // Grant access: update metadata lists
                    if(msg.grant_write){
                        if(!list_contains(writers,msg.target)) list_append(writers,sizeof(writers),msg.target);
                        // Ensure writers are also listed as readers
                        if(!list_contains(readers,msg.target)) list_append(readers,sizeof(readers),msg.target);
                    } else {
                        if(!list_contains(readers,msg.target) && !list_contains(writers,msg.target)) list_append(readers,sizeof(readers),msg.target);
                    }
                    time_t now=time(NULL); updated=(long long)now;
                    FILE* mw=fopen(meta,"w"); if(!mw){ nm_send_error(500,"Meta write failed"); break; }
                    fprintf(mw,"owner:%s\ncreated:%lld\nupdated:%lld\n", owner, created, updated);
                    if(accessed_keep>0){ fprintf(mw, "accessed:%lld\n", accessed_keep); }
                    fprintf(mw,"readers:%s%s\n", (readers[0]?" ":""), readers);
                    fprintf(mw,"writers:%s%s\n", (writers[0]?" ":""), writers);
                    fclose(mw);
                }
                remove(rpath); // remove request regardless of approve/deny
                MsgHeader ack={ .command=CMD_ACK, .payload_size=0 }; LOG_SS("[NM] RESPOND_REQUEST ACK file='%s' target='%s' approve=%d write=%d\n", msg.filename, msg.target, msg.approve, msg.grant_write); send_all(nm_socket,&ack,sizeof(ack));
                break;
            }
            // --- CHECKPOINT CREATE ---
            case CMD_CHECKPOINT_CREATE: {
                if (header.payload_size != sizeof(MsgCheckpointCreate)) { nm_send_error(400, "Bad payload size"); break; }
                MsgCheckpointCreate msg; if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(msg.filename)) { nm_send_error(400, "Invalid filename"); break; }
                // Validate tag (allow [A-Za-z0-9-_], length 1..64)
                size_t tlen = strnlen(msg.tag, sizeof(msg.tag));
                if (tlen == 0 || tlen > 64) { nm_send_error(400, "Invalid tag length"); break; }
                int tag_ok = 1; for (size_t i=0;i<tlen;i++){ char c=msg.tag[i]; if(!(isalnum((unsigned char)c)||c=='-'||c=='_')){ tag_ok=0; break; }}
                if (!tag_ok) { nm_send_error(400, "Invalid tag chars"); break; }
                // ACL: owner or writer can create checkpoint
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; char writers[1024]={0}; char line[2048]; while(fgets(line,sizeof(line),mf)){ if(strncmp(line,"owner:",6)==0){ sscanf(line+6, "%63s", owner);} else if(strncmp(line,"writers:",8)==0){ strncpy(writers,line+8,sizeof(writers)-1);} } fclose(mf); trim_both(writers);
                if(!( (owner[0]&&msg.requester[0]&&strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0) || list_contains(writers,msg.requester) )){ nm_send_error(403,"Write access denied"); break; }
                // Ensure no active sentence locks on this file to keep snapshot consistent
                if (sentence_lock_any_for_file(msg.filename)) { nm_send_error(423, "Active write lock"); break; }
                // Prepare checkpoint directory
                char chkroot[512]; snprintf(chkroot,sizeof(chkroot), STORAGE_ROOT "/.checkpoints"); if(ensure_dir(STORAGE_ROOT)!=0 || ensure_dir(chkroot)!=0){ nm_send_error(500,"Storage dir error"); break; }
                char filedir[512]; snprintf(filedir,sizeof(filedir), STORAGE_ROOT "/.checkpoints/%s", msg.filename); if(ensure_dir(filedir)!=0){ nm_send_error(500,"Checkpoint dir error"); break; }
                // Source file path
                char srcpath[512]; snprintf(srcpath,sizeof(srcpath), STORAGE_ROOT "/%s", msg.filename); FILE* src=fopen(srcpath,"rb"); if(!src){ nm_send_error(404,"File not found"); break; }
                // Destination checkpoint path (tag)
                char dstpath[700]; snprintf(dstpath,sizeof(dstpath), "%s/%s", filedir, msg.tag);
                // Fail if already exists
                if (file_exists(dstpath)) { fclose(src); nm_send_error(409, "Tag exists"); break; }
                FILE* dst=fopen(dstpath,"wb"); if(!dst){ fclose(src); nm_send_error(500,"Open checkpoint failed"); break; }
                char buf[4096]; size_t n; while((n=fread(buf,1,sizeof(buf),src))>0){ if(fwrite(buf,1,n,dst)!=n){ fclose(src); fclose(dst); nm_send_error(500,"Write checkpoint failed"); goto chk_create_end; }}
                fclose(src); fclose(dst);
                {
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 }; LOG_SS("[NM] CHECKPOINT_CREATE ACK '%s' tag='%s'\n", msg.filename, msg.tag); send_all(nm_socket,&ack,sizeof(ack));
                }
chk_create_end:
                break;
            }
            // --- CHECKPOINT LIST ---
            case CMD_CHECKPOINT_LIST: {
                if (header.payload_size != sizeof(MsgCheckpointList)) { nm_send_error(400, "Bad payload size"); break; }
                MsgCheckpointList msg; if(!recv_all(nm_socket,&msg,sizeof(msg))){ nm_send_error(400,"Payload read failed"); break; }
                if(!is_valid_filename(msg.filename)){ nm_send_error(400,"Invalid filename"); break; }
                // ACL: read permission (owner/readers/writers)
                char meta[512]; snprintf(meta,sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; char readers[1024]={0}; char writers[1024]={0}; char line[2048]; while(fgets(line,sizeof(line),mf)){ if(strncmp(line,"owner:",6)==0){ sscanf(line+6,"%63s",owner);} else if(strncmp(line,"readers:",8)==0){ strncpy(readers,line+8,sizeof(readers)-1);} else if(strncmp(line,"writers:",8)==0){ strncpy(writers,line+8,sizeof(writers)-1);} } fclose(mf); trim_both(readers); trim_both(writers);
                if(!( (owner[0]&&msg.requester[0]&&strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0) || list_contains(readers,msg.requester) || list_contains(writers,msg.requester) )){ nm_send_error(403,"Unauthorized"); break; }
                char filedir[512]; snprintf(filedir,sizeof(filedir), STORAGE_ROOT "/.checkpoints/%s", msg.filename);
                MsgCheckpointListResponse resp = {0};
                DIR* d = opendir(filedir);
                if(d){ struct dirent* ent; size_t pos=0; while((ent=readdir(d))!=NULL){ if(strcmp(ent->d_name,".")==0||strcmp(ent->d_name,"..")==0) continue; size_t ln=strlen(ent->d_name); if(pos+ln+1 < sizeof(resp.tags)){ memcpy(resp.tags+pos, ent->d_name, ln); pos+=ln; resp.tags[pos++]='\n'; } }
                    closedir(d);
                }
                MsgHeader hresp = { .command = CMD_CHECKPOINT_LIST_RESP, .payload_size = sizeof(resp) }; LOG_SS("[NM] CHECKPOINT_LIST RESP '%s' bytes=%zu\n", msg.filename, sizeof(resp)); send_all(nm_socket,&hresp,sizeof(hresp)); send_all(nm_socket,&resp,sizeof(resp));
                break;
            }
            // --- CHECKPOINT VIEW ---
            case CMD_CHECKPOINT_VIEW: {
                if(header.payload_size != sizeof(MsgCheckpointView)){ nm_send_error(400,"Bad payload size"); break; }
                MsgCheckpointView msg; if(!recv_all(nm_socket,&msg,sizeof(msg))){ nm_send_error(400,"Payload read failed"); break; }
                if(!is_valid_filename(msg.filename)){ nm_send_error(400,"Invalid filename"); break; }
                size_t tlen=strnlen(msg.tag,sizeof(msg.tag)); if(tlen==0||tlen>64){ nm_send_error(400,"Invalid tag"); break; }
                int tag_ok=1; for(size_t i=0;i<tlen;i++){ char c=msg.tag[i]; if(!(isalnum((unsigned char)c)||c=='-'||c=='_')){ tag_ok=0; break; }} if(!tag_ok){ nm_send_error(400,"Invalid tag chars"); break; }
                // ACL read
                char meta[512]; snprintf(meta,sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; char readers[1024]={0}; char writers[1024]={0}; char line[2048]; while(fgets(line,sizeof(line),mf)){ if(strncmp(line,"owner:",6)==0){ sscanf(line+6,"%63s",owner);} else if(strncmp(line,"readers:",8)==0){ strncpy(readers,line+8,sizeof(readers)-1);} else if(strncmp(line,"writers:",8)==0){ strncpy(writers,line+8,sizeof(writers)-1);} } fclose(mf); trim_both(readers); trim_both(writers);
                if(!( (owner[0]&&msg.requester[0]&&strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0) || list_contains(readers,msg.requester) || list_contains(writers,msg.requester) )){ nm_send_error(403,"Unauthorized"); break; }
                char chkpath[700]; snprintf(chkpath,sizeof(chkpath), STORAGE_ROOT "/.checkpoints/%s/%s", msg.filename, msg.tag); struct stat st; if(stat(chkpath,&st)!=0||!S_ISREG(st.st_mode)){ nm_send_error(404,"Checkpoint not found"); break; }
                FILE* f=fopen(chkpath,"rb"); if(!f){ nm_send_error(500,"Open failed"); break; }
                MsgHeader ack = { .command = CMD_ACK, .payload_size = (int)st.st_size }; LOG_SS("[NM] CHECKPOINT_VIEW ACK '%s' tag='%s' size=%d\n", msg.filename, msg.tag, ack.payload_size); if(!send_all(nm_socket,&ack,sizeof(ack))){ fclose(f); break; }
                char buf[4096]; size_t rem = (size_t)st.st_size; while(rem>0){ size_t chunk = rem>sizeof(buf)?sizeof(buf):rem; size_t nr=fread(buf,1,chunk,f); if(nr==0) break; if(!send_all(nm_socket,buf,nr)) break; rem -= nr; }
                fclose(f);
                break;
            }
            // --- CHECKPOINT REVERT ---
            case CMD_CHECKPOINT_REVERT: {
                if(header.payload_size != sizeof(MsgCheckpointRevert)){ nm_send_error(400,"Bad payload size"); break; }
                MsgCheckpointRevert msg; if(!recv_all(nm_socket,&msg,sizeof(msg))){ nm_send_error(400,"Payload read failed"); break; }
                if(!is_valid_filename(msg.filename)){ nm_send_error(400,"Invalid filename"); break; }
                size_t tlen=strnlen(msg.tag,sizeof(msg.tag)); if(tlen==0||tlen>64){ nm_send_error(400,"Invalid tag"); break; }
                int tag_ok=1; for(size_t i=0;i<tlen;i++){ char c=msg.tag[i]; if(!(isalnum((unsigned char)c)||c=='-'||c=='_')){ tag_ok=0; break; }} if(!tag_ok){ nm_send_error(400,"Invalid tag chars"); break; }
                // ACL: owner or writer; also ensure no active sentence locks
                char meta[512]; snprintf(meta,sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename); FILE* mf=fopen(meta,"r"); if(!mf){ nm_send_error(404,"File not found"); break; }
                char owner[MAX_USERNAME_LEN]={0}; char writers[1024]={0}; char line[2048]; while(fgets(line,sizeof(line),mf)){ if(strncmp(line,"owner:",6)==0){ sscanf(line+6,"%63s", owner);} else if(strncmp(line,"writers:",8)==0){ strncpy(writers,line+8,sizeof(writers)-1);} } fclose(mf); trim_both(writers);
                if(!( (owner[0]&&msg.requester[0]&&strncmp(owner,msg.requester,MAX_USERNAME_LEN)==0) || list_contains(writers,msg.requester) )){ nm_send_error(403,"Write access denied"); break; }
                if(sentence_lock_any_for_file(msg.filename)){ nm_send_error(423,"Active write lock"); break; }
                char chkpath[700]; snprintf(chkpath,sizeof(chkpath), STORAGE_ROOT "/.checkpoints/%s/%s", msg.filename, msg.tag); struct stat st; if(stat(chkpath,&st)!=0||!S_ISREG(st.st_mode)){ nm_send_error(404,"Checkpoint not found"); break; }
                // Replace content file atomically via temp
                char content[512]; snprintf(content,sizeof(content), STORAGE_ROOT "/%s", msg.filename); FILE* src=fopen(chkpath,"rb"); if(!src){ nm_send_error(500,"Open failed"); break; }
                char tmppath[700]; snprintf(tmppath,sizeof(tmppath), "%s.tmp", content); FILE* dst=fopen(tmppath,"wb"); if(!dst){ fclose(src); nm_send_error(500,"Tmp failed"); break; }
                char buf[4096]; size_t n; while((n=fread(buf,1,sizeof(buf),src))>0){ if(fwrite(buf,1,n,dst)!=n){ fclose(src); fclose(dst); remove(tmppath); nm_send_error(500,"Write failed"); goto chk_revert_end; }} fclose(src); fclose(dst); if(rename(tmppath,content)!=0){ remove(tmppath); nm_send_error(500,"Replace failed"); goto chk_revert_end; }
                // Update metadata 'updated' while preserving 'accessed' and ACL
                {
                    char owner_k[MAX_USERNAME_LEN]={0}; long long created_k=0; long long accessed_keep=0; char readers_k[1024]={0}, writers_k[1024]={0};
                    FILE* mfr=fopen(meta,"r"); if(mfr){ char lb[2048]; while(fgets(lb,sizeof(lb),mfr)){
                        if(strncmp(lb,"owner:",6)==0){ sscanf(lb+6, "%63s", owner_k); }
                        else if(strncmp(lb,"created:",8)==0){ sscanf(lb+8, "%lld", &created_k); }
                        else if(strncmp(lb,"accessed:",9)==0){ long long t=0; if(sscanf(lb+9, "%lld", &t)==1 && t>accessed_keep) accessed_keep=t; }
                        else if(strncmp(lb,"readers:",8)==0){ strncpy(readers_k,lb+8,sizeof(readers_k)-1); }
                        else if(strncmp(lb,"writers:",8)==0){ strncpy(writers_k,lb+8,sizeof(writers_k)-1); }
                    } fclose(mfr); trim_both(readers_k); trim_both(writers_k);} FILE* mfw=fopen(meta,"w"); if(mfw){ time_t now=time(NULL); fprintf(mfw,"owner:%s\ncreated:%lld\nupdated:%lld\n", owner_k, created_k, (long long)now); if(accessed_keep>0) fprintf(mfw,"accessed:%lld\n", accessed_keep); fprintf(mfw,"readers:%s%s\n", (readers_k[0]?" ":""), readers_k); fprintf(mfw,"writers:%s%s\n", (writers_k[0]?" ":""), writers_k); fclose(mfw);} }
                MsgHeader ack={ .command=CMD_ACK, .payload_size=0 }; LOG_SS("[NM] CHECKPOINT_REVERT ACK '%s' tag='%s'\n", msg.filename, msg.tag); send_all(nm_socket,&ack,sizeof(ack));
chk_revert_end:
                break;
            }
            default:
                // TODO: other commands in Phase 2
                break;
        }
    }
    return NULL;
}

int main(int argc, char const *argv[]) {
    // Avoid termination on broken pipe when NM disconnects
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2) {
    LOGE_SS("Usage: %s <client_listen_port> [nm_ip_address]\n", argv[0]);
        return 1;
    }

    client_listen_port = atoi(argv[1]);
    if (client_listen_port <= 0 || client_listen_port > 65535) {
    LOGE_SS("Invalid port number.\n");
        return 1;
    }

    // Parse Name Server IP (default to localhost)
    const char* nm_ip = (argc >= 3) ? argv[2] : "127.0.0.1";

    // --- Start Thread to Listen for Clients ---
    pthread_t client_thread_id;
    if (pthread_create(&client_thread_id, NULL, handle_client_connections, NULL) != 0) {
        perror("[SS] Failed to create client listener thread");
        return 1;
    }

    // --- Connect to Name Server ---
    struct sockaddr_in nm_address;
    if ((nm_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("[SS] Socket creation error");
        return 1;
    }

    nm_address.sin_family = AF_INET;
    nm_address.sin_port = htons(NAME_SERVER_PORT);

    // Convert IPv4 address from text to binary form
    if (inet_pton(AF_INET, nm_ip, &nm_address.sin_addr) <= 0) {
        perror("[SS] Invalid address/ Address not supported");
        return 1;
    }

    LOG_SS("Connecting to Name Server at %s:%d...\n", nm_ip, NAME_SERVER_PORT);

    if (connect(nm_socket, (struct sockaddr *)&nm_address, sizeof(nm_address)) < 0) {
        perror("[SS] Connection Failed");
        return 1;
    }

    LOG_SS("Connected to Name Server!\n");

    // Ensure storage root exists
    if (ensure_dir(STORAGE_ROOT) != 0) {
    LOGE_SS("Failed to ensure storage root '%s'\n", STORAGE_ROOT);
        return 1;
    }

    // --- Register with Name Server ---
    MsgHeader header;
    MsgRegisterSS reg_msg;

    memset(&reg_msg, 0, sizeof(reg_msg));
    header.command = CMD_REGISTER_SS;
    header.payload_size = sizeof(MsgRegisterSS);
    reg_msg.client_listen_port = client_listen_port;

    // Build initial file list (newline-separated) as a best-effort snapshot
    DIR* d = opendir(STORAGE_ROOT);
    if (d) {
        struct dirent* de;
        size_t used = 0;
        while ((de = readdir(d)) != NULL) {
            const char* name = de->d_name;
            if (!name || name[0] == '.') continue; // skip . and .. and hidden
            // Skip metadata files (*.meta) and any directories
            const char* dot = strrchr(name, '.');
            if (dot && strcmp(dot, ".meta") == 0) continue;
            // Build full path and ensure it's a regular file
            char pbuf[512];
            snprintf(pbuf, sizeof(pbuf), STORAGE_ROOT "/%s", name);
            struct stat st;
            if (stat(pbuf, &st) != 0 || !S_ISREG(st.st_mode)) continue;

            size_t len = strlen(name);
            if (len + 1 > sizeof(reg_msg.initial_files) - used - 1) break;
            memcpy(reg_msg.initial_files + used, name, len);
            used += len;
            reg_msg.initial_files[used++] = '\n';
            reg_msg.initial_files[used] = '\0';
        }
        closedir(d);
    }

    LOG_SS("Registering with NM (Client Port: %d, %sinitial file list sent)...\n",
           client_listen_port,
           reg_msg.initial_files[0] ? "" : "no ");

    // Send header, then payload
    if (!send_all(nm_socket, &header, sizeof(MsgHeader))) return 1;
    if (!send_all(nm_socket, &reg_msg, sizeof(MsgRegisterSS))) return 1;


    // --- Wait for ACK ---
    if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
    LOGE_SS("Failed to receive ACK from NM.\n");
        return 1;
    }

    if (header.command == CMD_ACK) {
    LOG_SS("Registration successful!\n");
    } else {
    LOGE_SS("Registration failed (Received command %d).\n", header.command);
        return 1;
    }
    
    // --- Start Thread to Listen for NM Commands ---
    pthread_t nm_thread_id;
    if(pthread_create(&nm_thread_id, NULL, listen_to_nm, NULL) != 0) {
        perror("[SS] Failed to create NM listener thread");
        return 1;
    }

    // Wait for threads to finish (which they won't)
    pthread_join(client_thread_id, NULL);
    pthread_join(nm_thread_id, NULL);
    
    close(nm_socket);
    return 0;
}
