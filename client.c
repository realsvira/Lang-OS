#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <poll.h>
#include <strings.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>

int nm_socket;
char username[MAX_USERNAME_LEN];

// Watchdog thread: exits the process as soon as the NM socket is closed,
// without waiting for the next command round-trip.
void* nm_watchdog(void* arg) {
    (void)arg;
    while (1) {
        struct pollfd pfd;
        pfd.fd = nm_socket;
        pfd.events = POLLIN | POLLHUP | POLLERR;
        pfd.revents = 0;

        int r = poll(&pfd, 1, 500); // 500ms heartbeat
        if (r < 0) {
            LOGE_CLIENT("poll failed: %s\n", strerror(errno));
            // On poll error, be conservative and exit
            LOGE_CLIENT("NM watchdog exiting due to poll error.\n");
            _exit(1);
        }
        if (r == 0) {
            continue; // timeout, loop again
        }
        if (pfd.revents & (POLLHUP | POLLERR)) {
            LOGE_CLIENT("Name Server connection closed. Exiting.\n");
            _exit(1);
        }
        if (pfd.revents & POLLIN) {
            // Non-consuming peek to detect EOF (0 means closed)
            char ch;
            ssize_t n = recv(nm_socket, &ch, 1, MSG_PEEK);
            if (n == 0) {
                LOGE_CLIENT("Name Server disconnected. Exiting.\n");
                _exit(1);
            }
            // n > 0 means data pending; ignore (main thread will read it)
        }
    }
    return NULL;
}

// Removed broken listen_to_nm placeholder from merge; NM interactions are synchronous in the REPL.

// Helper function to remove trailing newline from fgets
void remove_newline(char* str) {
    str[strcspn(str, "\n")] = 0;
}

// Portable strdup to avoid missing prototype issues on some platforms
static char* safe_strdup(const char* s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char* p = (char*)malloc(n);
    if (!p) return NULL;
    memcpy(p, s, n);
    return p;
}

int main(int argc, char** argv) {
    // Ensure sends to closed sockets don't kill the client process
    signal(SIGPIPE, SIG_IGN);

    // --- Parse Command-Line Arguments ---
    const char* nm_ip = "127.0.0.1"; // Default to localhost
    if (argc >= 2) {
        nm_ip = argv[1];
    }

    // --- Get Username ---
    if (_term_supports_color_file(stdout)) {
        printf("\033[1;36mEnter your username:\033[0m ");
    } else {
        printf("Enter your username: ");
    }
    if (fgets(username, MAX_USERNAME_LEN, stdin) == NULL) {
    LOGE_CLIENT("Error reading username.\n");
        return 1;
    }
    remove_newline(username);
    
    if (strlen(username) == 0) {
    LOGE_CLIENT("Invalid username.\n");
        return 1;
    }

    // --- Connect to Name Server ---
    struct sockaddr_in nm_address;
    if ((nm_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOGE_CLIENT("Socket creation error: %s\n", strerror(errno));
        return 1;
    }

    nm_address.sin_family = AF_INET;
    nm_address.sin_port = htons(NAME_SERVER_PORT);
    if (inet_pton(AF_INET, nm_ip, &nm_address.sin_addr) <= 0) {
        LOGE_CLIENT("Invalid address: %s\n", nm_ip);
        return 1;
    }

    LOG_CLIENT("Connecting to Name Server at %s:%d...\n", nm_ip, NAME_SERVER_PORT);
    if (connect(nm_socket, (struct sockaddr *)&nm_address, sizeof(nm_address)) < 0) {
        LOGE_CLIENT("Connection Failed: %s\n", strerror(errno));
        return 1;
    }
    LOG_CLIENT("Connected to Name Server!\n");

    // --- Register with Name Server ---
    MsgHeader header;
    MsgRegisterClient reg_msg;

    memset(&reg_msg, 0, sizeof(reg_msg));
    header.command = CMD_REGISTER_CLIENT;
    header.payload_size = sizeof(MsgRegisterClient);

    // Fill username
    strncpy(reg_msg.username, username, MAX_USERNAME_LEN - 1);
    reg_msg.username[MAX_USERNAME_LEN - 1] = '\0';

    // Derive client IP as seen on this socket (best-effort)
    struct sockaddr_in local_addr;
    socklen_t la_len = sizeof(local_addr);
    char cip[MAX_IP_LEN] = {0};
    if (getsockname(nm_socket, (struct sockaddr*)&local_addr, &la_len) == 0) {
        inet_ntop(AF_INET, &local_addr.sin_addr, cip, sizeof(cip));
    }
    if (cip[0]) {
        strncpy(reg_msg.client_ip, cip, MAX_IP_LEN - 1);
        reg_msg.client_ip[MAX_IP_LEN - 1] = '\0';
    }

    // NM port is known from configuration
    reg_msg.nm_port = NAME_SERVER_PORT;
    // Provide a concrete local port (best-effort) for bookkeeping, as per spec
    reg_msg.ss_port = (int)ntohs(local_addr.sin_port);

    LOG_CLIENT("Registering as '%s' (ip=%s, nm_port=%d, ss_port=%d)...\n",
               username,
               reg_msg.client_ip[0] ? reg_msg.client_ip : "",
               reg_msg.nm_port,
               reg_msg.ss_port);
    if (!send_all(nm_socket, &header, sizeof(MsgHeader))) return 1;
    if (!send_all(nm_socket, &reg_msg, sizeof(MsgRegisterClient))) return 1;
    
    // --- Wait for ACK ---
    if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
        LOGE_CLIENT("Failed to receive ACK from NM.\n");
        return 1;
    }
    if (header.command == CMD_ACK) {
        LOG_CLIENT("Registration successful!\n");
    } else {
        LOGE_CLIENT("Registration failed.\n");
        return 1;
    }

    // --- Start NM disconnect watchdog (proactive exit on NM down) ---
    pthread_t wd_thread_id;
    if (pthread_create(&wd_thread_id, NULL, nm_watchdog, NULL) != 0) {
        LOGE_CLIENT("Failed to create NM watchdog thread: %s\n", strerror(errno));
        // Non-fatal: client will still exit on next NM interaction
    } else {
        pthread_detach(wd_thread_id);
    }

    // --- MAIN REPL (Read-Eval-Print Loop) ---
    char line[1024];
    while (true) {
        if (_term_supports_color_file(stdout)) {
            printf("\033[1;36m%s>\033[0m ", username);
        } else {
            printf("%s> ", username);
        }
        fflush(stdout);
        
        if (fgets(line, sizeof(line), stdin) == NULL) {
            break; // EOF (e.g., Ctrl+D)
        }
        remove_newline(line);
        if (strlen(line) == 0) {
            continue;
        }

        // Parse command using strtok
        char* command = strtok(line, " ");

        if (command == NULL) {
            continue;
        }

        if (strcmp(command, "exit") == 0) {
            break;
        } else if (strcmp(command, "CREATE") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: CREATE <filename>\n");
                continue;
            }

            MsgHeader h = {0};
            MsgCreateFile payload = {0};
            h.command = CMD_CREATE_FILE;
            h.payload_size = sizeof(MsgCreateFile);
            strncpy(payload.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(payload.owner, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) ||
                !send_all(nm_socket, &payload, sizeof(payload))) {
                LOGE_CLIENT("Failed to send CREATE to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                LOGE_CLIENT("No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }

            if (rh.command == CMD_ACK) {
                printf("[Client] File '%s' created.\n", fname);
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) &&
                    recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] CREATE failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] CREATE failed (unknown error).\n");
                }
            } else {
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
    } else if (strcmp(command, "DELETE") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: DELETE <filename>\n");
                continue;
            }

            MsgHeader h = {0};
            MsgDeleteFile payload = {0};
            h.command = CMD_DELETE_FILE;
            h.payload_size = sizeof(MsgDeleteFile);
            strncpy(payload.filename, fname, MAX_FILENAME_LEN - 1);
            // requester will be set by NM; we can set it here for completeness
            strncpy(payload.requester, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) ||
                !send_all(nm_socket, &payload, sizeof(payload))) {
                LOGE_CLIENT("Failed to send DELETE to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                LOGE_CLIENT("No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }

            if (rh.command == CMD_ACK) {
                printf("[Client] File '%s' deleted.\n", fname);
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) &&
                    recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] DELETE failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] DELETE failed (unknown error).\n");
                }
            } else {
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "CLEAR") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: CLEAR <filename>\n");
                continue;
            }

            MsgHeader h = {0};
            MsgClearFile payload = {0};
            h.command = CMD_CLEAR_FILE;
            h.payload_size = sizeof(MsgClearFile);
            strncpy(payload.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(payload.requester, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) ||
                !send_all(nm_socket, &payload, sizeof(payload))) {
                LOGE_CLIENT("Failed to send CLEAR to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                LOGE_CLIENT("No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }

            if (rh.command == CMD_ACK) {
                printf("[Client] File '%s' cleared.\n", fname);
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) && recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] CLEAR failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] CLEAR failed (unknown error).\n");
                }
            } else {
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "VIEW") == 0) {
            // VIEW flags: -a (all), -l (long listing). Accept combinations: -a, -l, -al, -la
            // Default behavior (per spec): list only files the user has access to
            int show_all = 0, long_list = 0;
            char* flag = strtok(NULL, " ");
            if (flag) {
                if (strcmp(flag, "-a") == 0) { show_all = 1; }
                else if (strcmp(flag, "-l") == 0) { long_list = 1; }
                else if (strcmp(flag, "-al") == 0 || strcmp(flag, "-la") == 0) { show_all = 1; long_list = 1; }
                else {
                    printf("[Client] Usage: VIEW [-a|-l|-al]\n");
                }
            }

            MsgViewFilesRequest rq = {0};
            rq.show_all = show_all;
            rq.long_list = long_list;
            strncpy(rq.requester, username, MAX_USERNAME_LEN - 1);
            MsgHeader h = {0};
            h.command = CMD_VIEW_FILES;
            h.payload_size = sizeof(rq);

            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                LOGE_CLIENT("Failed to send VIEW to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                LOGE_CLIENT("No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }

            if (rh.command == CMD_VIEW_FILES_RESP && rh.payload_size == sizeof(MsgViewFilesResponse)) {
                MsgViewFilesResponse resp = {0};
                if (!recv_all(nm_socket, &resp, sizeof(resp))) {
                    LOGE_CLIENT("Failed to read VIEW payload.\n");
                    break; // Treat as NM disconnect and exit
                }
                if (resp.file_list[0] == '\0') {
                    printf("[Client] No files available.\n");
                } else {
                    printf("%s", resp.file_list);
                }
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) && recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] VIEW failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] VIEW failed (unknown error).\n");
                }
            } else {
                // Drain any unexpected payload
                size_t remaining = rh.payload_size;
                char drain[512];
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                    if (!recv_all(nm_socket, drain, chunk)) break;
                    remaining -= chunk;
                }
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "TRASH") == 0) {
            // List current user's trash across all SS
            MsgTrashList rq = {0}; strncpy(rq.owner, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_TRASH_LIST, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) { LOGE_CLIENT("Failed to send TRASH list.\n"); continue; }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_TRASH_LIST_RESP && rh.payload_size == sizeof(MsgTrashListResp)) {
                MsgTrashListResp resp = {0}; if (!recv_all(nm_socket, &resp, sizeof(resp))) { LOGE_CLIENT("Failed to read TRASH payload.\n"); break; }
                if (resp.files[0] == '\0') printf("[Client] Trash is empty.\n"); else printf("%s", resp.files);
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] TRASH failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, ch)) break; rem -= ch; }
                printf("[Client] Unexpected response to TRASH (%d).\n", rh.command);
            }
        } else if (strcmp(command, "RECOVER") == 0) {
            // RECOVER <filename> [newname]
            char* fname = strtok(NULL, " "); char* newname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: RECOVER <filename> [newname]\n"); continue; }
            MsgTrashRecover rq = {0}; strncpy(rq.owner, username, MAX_USERNAME_LEN-1); strncpy(rq.filename, fname, MAX_FILENAME_LEN-1); if (newname) strncpy(rq.newname, newname, MAX_FILENAME_LEN-1);
            MsgHeader h = { .command = CMD_TRASH_RECOVER, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) { LOGE_CLIENT("Failed to send RECOVER.\n"); continue; }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) { printf("[Client] Recovered '%s'%s%s.\n", fname, newname?" as ":"", newname?newname:""); }
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] RECOVER failed (%d): %s\n", err.code, err.message); }
            else { size_t rem = rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response to RECOVER (%d).\n", rh.command); }
        } else if (strcmp(command, "EMPTYTRASH") == 0) {
            // EMPTYTRASH [-all | <filename>]
            char* arg = strtok(NULL, " ");
            MsgTrashEmpty rq = {0}; strncpy(rq.owner, username, MAX_USERNAME_LEN-1);
            if (!arg || strcasecmp(arg, "-all") == 0) { rq.all = 1; rq.filename[0] = '\0'; }
            else { rq.all = 0; strncpy(rq.filename, arg, MAX_FILENAME_LEN-1); }
            MsgHeader h = { .command = CMD_TRASH_EMPTY, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) { LOGE_CLIENT("Failed to send EMPTYTRASH.\n"); continue; }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) { if (rq.all) printf("[Client] Emptied trash.\n"); else printf("[Client] Removed '%s' from trash.\n", rq.filename); }
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] EMPTYTRASH failed (%d): %s\n", err.code, err.message); }
            else { size_t rem=rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response to EMPTYTRASH (%d).\n", rh.command); }
        } else if (strcmp(command, "WRITE") == 0) {
            char* fname = strtok(NULL, " ");
            char* sidx_s = strtok(NULL, " ");
            if (!fname || !sidx_s) {
                printf("[Client] Usage: WRITE <filename> <sentence_number>\n");
                continue;
            }
            // Use 0-based sentence index (as per your workflow)
            int sidx = atoi(sidx_s);
            if (sidx < 0) { printf("[Client] Invalid sentence index.\n"); continue; }

            // Resolve SS address for this file via NM (reuse READ routing)
            MsgHeader h = (MsgHeader){0};
            MsgReadFile req = (MsgReadFile){0};
            h.command = CMD_READ_FILE;
            h.payload_size = sizeof(req);
            strncpy(req.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(req.requester, username, MAX_USERNAME_LEN - 1);
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &req, sizeof(req))) {
                LOGE_CLIENT("Failed to query NM for file location.\n");
                continue;
            }
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM no response.\n"); continue; }
            if (rh.command != CMD_READ_FILE_RESP || rh.payload_size != sizeof(MsgReadFileResponse)) {
                // Drain
                size_t rem = rh.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected NM response (%d).\n", rh.command);
                continue;
            }
            MsgReadFileResponse addr = {0};
            if (!recv_all(nm_socket, &addr, sizeof(addr))) { LOGE_CLIENT("Failed reading location.\n"); continue; }
            if (!addr.found) { printf("[Client] File not found.\n"); continue; }

            // Connect to SS (header-based protocol)
            int ss_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (ss_fd < 0) { perror("[Client] socket"); continue; }
            struct sockaddr_in ss_addr; memset(&ss_addr, 0, sizeof(ss_addr));
            ss_addr.sin_family = AF_INET; ss_addr.sin_port = htons((uint16_t)addr.ss_port);
            if (inet_pton(AF_INET, addr.ss_ip, &ss_addr.sin_addr) <= 0) { perror("[Client] inet_pton"); close(ss_fd); continue; }
            if (connect(ss_fd, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) { perror("[Client] connect SS"); close(ss_fd); continue; }

            // Acquire lock: CMD_WRITE_BEGIN
            MsgWriteBegin begin = (MsgWriteBegin){0}; strncpy(begin.filename, fname, MAX_FILENAME_LEN - 1); begin.sentence_index = sidx; strncpy(begin.requester, username, MAX_USERNAME_LEN - 1);
            MsgHeader bh = { .command = CMD_WRITE_BEGIN, .payload_size = sizeof(begin) };
            if (!send_all(ss_fd, &bh, sizeof(bh)) || !send_all(ss_fd, &begin, sizeof(begin))) { LOGE_CLIENT("Failed to send WRITE_BEGIN.\n"); close(ss_fd); continue; }
            MsgHeader brh; if (!recv_all(ss_fd, &brh, sizeof(brh))) { LOGE_CLIENT("No response for WRITE_BEGIN.\n"); close(ss_fd); continue; }
            if (brh.command == CMD_ERROR && brh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] WRITE_BEGIN failed (%d): %s\n", err.code, err.message); close(ss_fd); continue;
            } else if (brh.command != CMD_ACK) {
                // Drain
                size_t rem = brh.payload_size; char drain[256]; while (rem > 0) { size_t chunk = rem > sizeof(drain)?sizeof(drain):rem; if (!recv_all(ss_fd, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response to WRITE_BEGIN (%d).\n", brh.command); close(ss_fd); continue;
            }

            // Fetch the file content from SS to build a working copy of the target sentence
            MsgHeader rqh = (MsgHeader){ .command = CMD_READ_FILE, .payload_size = sizeof(MsgReadFile) };
            if (!send_all(ss_fd, &rqh, sizeof(rqh)) || !send_all(ss_fd, &req, sizeof(req))) { LOGE_CLIENT("Failed to request file from SS.\n"); goto write_cancel_release; }
            MsgHeader rrh; if (!recv_all(ss_fd, &rrh, sizeof(rrh))) { LOGE_CLIENT("No READ response from SS.\n"); goto write_cancel_release; }
            if (rrh.command == CMD_ERROR && rrh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] READ failed (%d): %s\n", err.code, err.message); goto write_cancel_release; }
            if (rrh.command != CMD_ACK || rrh.payload_size < 0) { printf("[Client] Unexpected READ response (%d).\n", rrh.command); goto write_cancel_release; }
            int fsize = rrh.payload_size; char* filebuf = (char*)malloc((size_t)fsize + 1); if (!filebuf) { LOGE_CLIENT("OOM.\n"); goto write_cancel_release; }
            if (fsize > 0 && !recv_all(ss_fd, filebuf, (size_t)fsize)) { LOGE_CLIENT("Failed to receive file data.\n"); free(filebuf); goto write_cancel_release; }
            filebuf[fsize] = '\0';

            // Parse into sentences ('.', '!', '?'), including trailing whitespace as part of the sentence block
            size_t i = 0, n = (size_t)fsize; int current = 0; size_t s_begin = 0, s_end = 0; int found = 0;
            while (i < n) {
                size_t start = i;
                while (i < n && filebuf[i] != '.' && filebuf[i] != '!' && filebuf[i] != '?') i++;
                if (i < n && (filebuf[i] == '.' || filebuf[i] == '!' || filebuf[i] == '?')) {
                    i++;
                    while (i < n && (filebuf[i] == ' ' || filebuf[i] == '\n' || filebuf[i] == '\t' || filebuf[i] == '\r')) i++;
                }
                size_t end = i;
                if (current == sidx) { s_begin = start; s_end = end; found = 1; break; }
                current++;
            }
            if (!found && sidx == current) { s_begin = s_end = n; found = 1; }
            if (!found) {
                printf("[Client] Sentence %d not found.\n", sidx);
                free(filebuf);
                goto write_cancel_release;
            }

            // Extract the sentence and tokenize by whitespace
            size_t slen = (s_end > s_begin ? (s_end - s_begin) : 0);
            char* sentence = (char*)malloc(slen + 1); if (!sentence) { LOGE_CLIENT("OOM.\n"); free(filebuf); goto write_cancel_release; }
            memcpy(sentence, filebuf + s_begin, slen); sentence[slen] = '\0';
            free(filebuf);

            // Build vector of words
            char** words = NULL; int wcount = 0; size_t cap = 0;
            char* tokbuf = safe_strdup(sentence); if (!tokbuf) { LOGE_CLIENT("OOM.\n"); free(sentence); goto write_cancel_release; }
            char* saveptr = NULL; for (char* t = strtok_r(tokbuf, " \t\r\n", &saveptr); t; t = strtok_r(NULL, " \t\r\n", &saveptr)) {
                if (wcount == (int)cap) { cap = cap ? cap*2 : 8; char** nw = realloc(words, cap * sizeof(char*)); if (!nw) { LOGE_CLIENT("OOM.\n"); break; } words = nw; }
                char* dup = safe_strdup(t); if (!dup) { LOGE_CLIENT("OOM.\n"); break; }
                words[wcount++] = dup;
            }
            free(tokbuf);

            // Show current words
            if (wcount > 0) {
                printf("[Client] Current sentence words (1-based):\n");
                for (int iw = 0; iw < wcount; iw++) printf("  %d: %s\n", iw+1, words[iw]);
            } else {
                printf("[Client] Current sentence is empty.\n");
            }
            printf("[Client] Enter '<word_index> <content>' to replace/append. Type ETIRW to finish.\n");

            // Edit loop
            while (1) {
                printf("WRITE> "); fflush(stdout);
                char line[4096]; if (!fgets(line, sizeof(line), stdin)) { printf("\n[Client] Input ended.\n"); break; }
                line[strcspn(line, "\n")] = 0;
                if (strcmp(line, "ETIRW") == 0) break;
                // parse index
                char* p = line; while (*p==' ') p++;
                if (!*p) continue;
                char* endptr = NULL; long idx1 = strtol(p, &endptr, 10);
                if (p == endptr || idx1 <= 0) { printf("[Client] Expected '<word_index> <content>'.\n"); continue; }
                while (*endptr==' ') endptr++;
                char* content = endptr; if (!content) content = "";

                // Split content into tokens (by whitespace) to allow multi-word insert/replace
                // Build a temp array of tokens
                char** newtoks = NULL; int nt = 0; size_t ncap = 0;
                char* cdup = safe_strdup(content); if (!cdup) { LOGE_CLIENT("OOM.\n"); continue; }
                char* csp = NULL; for (char* ct = strtok_r(cdup, " \t\r\n", &csp); ct; ct = strtok_r(NULL, " \t\r\n", &csp)) {
                    if (nt == (int)ncap) { ncap = ncap ? ncap*2 : 4; char** narr = realloc(newtoks, ncap * sizeof(char*)); if (!narr) { LOGE_CLIENT("OOM.\n"); nt = -1; break; } newtoks = narr; }
                    char* d = safe_strdup(ct); if (!d) { LOGE_CLIENT("OOM.\n"); nt = -1; break; }
                    newtoks[nt++] = d;
                }
                free(cdup);
                if (nt < 0) { // allocation failure
                    // free partial
                    for (int k = 0; k < nt; k++) free(newtoks[k]);
                    free(newtoks);
                    continue;
                }

                // Apply change: replace word at (idx1-1) with tokens, or append if idx1 == wcount+1
                if (idx1 == wcount + 1) {
                    // append tokens
                    if (nt == 0) { /* no-op */ }
                    else {
                        // ensure capacity
                        if (wcount + nt > (int)cap) { size_t need = (size_t)wcount + (size_t)nt; size_t ncap2 = cap ? cap : 8; while (ncap2 < need) ncap2 *= 2; char** narr = realloc(words, ncap2 * sizeof(char*)); if (!narr) { LOGE_CLIENT("OOM.\n"); goto free_newtoks; } words = narr; cap = ncap2; }
                        for (int k = 0; k < nt; k++) words[wcount++] = newtoks[k], newtoks[k] = NULL;
                    }
                } else if (idx1 >= 1 && idx1 <= wcount) {
                    int pos = (int)idx1 - 1;
                    // remove original at pos, insert nt tokens at pos
                    free(words[pos]);
                    // shift tail as needed for insert-many
                    if (nt == 1) {
                        words[pos] = newtoks[0]; newtoks[0] = NULL; // replace in-place
                    } else {
                        int tail = wcount - (pos + 1);
                        // ensure capacity
                        if ((size_t)(wcount - 1 + nt) > cap) { size_t need = (size_t)wcount - 1 + (size_t)nt; size_t ncap2 = cap ? cap : 8; while (ncap2 < need) ncap2 *= 2; char** narr = realloc(words, ncap2 * sizeof(char*)); if (!narr) { LOGE_CLIENT("OOM.\n"); goto free_newtoks; } words = narr; cap = ncap2; }
                        // make room: move tail right by (nt-1)
                        if (tail > 0) memmove(&words[pos + nt], &words[pos + 1], (size_t)tail * sizeof(char*));
                        // place new tokens
                        for (int k = 0; k < nt; k++) { words[pos + k] = newtoks[k]; newtoks[k] = NULL; }
                        wcount = wcount - 1 + nt;
                    }
                } else {
                    printf("[Client] Word index must be between 1 and %d (or %d to append).\n", wcount, wcount+1);
                }

free_newtoks:
                for (int k = 0; k < nt; k++) if (newtoks[k]) free(newtoks[k]);
                free(newtoks);

                // Show preview after each edit
                printf("[Client] Preview: ");
                for (int iw = 0; iw < wcount; iw++) {
                    if (iw) putchar(' ');
                    fputs(words[iw], stdout);
                }
                putchar('\n');
            }

            // If no change made (preview equals original), still allow write; but if truly empty and was empty, we can cancel.
            // Build final sentence string
            char final_sentence[2048] = {0};
            size_t off = 0;
            for (int iw = 0; iw < wcount; iw++) {
                if (iw && off < sizeof(final_sentence)-1) final_sentence[off++] = ' ';
                const char* ws = words[iw]; size_t len = strlen(ws);
                size_t room = sizeof(final_sentence)-1 - off; if (len > room) len = room;
                memcpy(final_sentence + off, ws, len); off += len; final_sentence[off] = '\0';
            }

            // Apply write with final_sentence
            {
                MsgWriteFile w = (MsgWriteFile){0};
                strncpy(w.filename, fname, MAX_FILENAME_LEN - 1);
                w.sentence_index = sidx;
                strncpy(w.replacement, final_sentence, sizeof(w.replacement)-1);
                strncpy(w.requester, username, MAX_USERNAME_LEN - 1);
                MsgHeader wh = { .command = CMD_WRITE_FILE, .payload_size = sizeof(w) };
                if (!send_all(ss_fd, &wh, sizeof(wh)) || !send_all(ss_fd, &w, sizeof(w))) {
                    LOGE_CLIENT("Failed to send WRITE.\n");
                } else {
                    MsgHeader wr; if (!recv_all(ss_fd, &wr, sizeof(wr))) { LOGE_CLIENT("No response to WRITE.\n"); }
                    else if (wr.command == CMD_ERROR && wr.payload_size == sizeof(MsgError)) { MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] WRITE failed (%d): %s\n", err.code, err.message); }
                    else if (wr.command != CMD_ACK) { printf("[Client] Unexpected response to WRITE (%d).\n", wr.command); }
                    else { printf("[Client] WRITE applied.\n"); }
                }
            }

            // Release lock and cleanup
write_cancel_release:
            {
                MsgWriteDone done = (MsgWriteDone){0}; strncpy(done.filename, fname, MAX_FILENAME_LEN - 1); done.sentence_index = sidx; strncpy(done.requester, username, MAX_USERNAME_LEN - 1);
                MsgHeader dh = { .command = CMD_WRITE_DONE, .payload_size = sizeof(done) };
                send_all(ss_fd, &dh, sizeof(dh)); send_all(ss_fd, &done, sizeof(done));
                MsgHeader drh; if (recv_all(ss_fd, &drh, sizeof(drh))) {
                    if (drh.command == CMD_ERROR && drh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] WRITE_DONE failed (%d): %s\n", err.code, err.message); }
                }
                // free resources if present
                // sentence/words may or may not be allocated depending on the jump path
            }
            // free local allocations if they exist (safe to call with NULL if we guarded above)
            // NOTE: We cannot reference 'words' and 'sentence' here if the jump happened before their declaration.
            // We'll rely on process lifetime cleanup for rare early goto; typical path cleans explicitly above.
            close(ss_fd);
        } else if (strcmp(command, "ADDACCESS") == 0) {
            // Spec: ADDACCESS -R|-W <filename> <username>
            // Backward compat: ADDACCESS <filename> <username> <R|W>
            char* flag = strtok(NULL, " ");
            char* fname = NULL; char* target = NULL; int is_writer = -1;
            if (flag && flag[0] == '-') {
                // New spec ordering
                if (strcasecmp(flag, "-R") == 0) is_writer = 0;
                else if (strcasecmp(flag, "-W") == 0) is_writer = 1;
                fname = strtok(NULL, " ");
                target = strtok(NULL, " ");
            } else {
                // Legacy ordering
                fname = flag;
                target = strtok(NULL, " ");
                char* mode = strtok(NULL, " ");
                if (mode) {
                    if (mode[0]=='R' || mode[0]=='r') is_writer = 0;
                    else if (mode[0]=='W' || mode[0]=='w') is_writer = 1;
                }
            }
            if (!fname || !target || !(is_writer==0 || is_writer==1)) {
                printf("[Client] Usage: ADDACCESS -R|-W <filename> <username>\n");
                continue;
            }
            MsgAccessChange ac = (MsgAccessChange){0};
            strncpy(ac.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(ac.target, target, MAX_USERNAME_LEN-1);
            strncpy(ac.requester, username, MAX_USERNAME_LEN-1);
            ac.is_writer = is_writer;
            MsgHeader h = { .command = CMD_ADD_ACCESS, .payload_size = sizeof(ac) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &ac, sizeof(ac))) {
                fprintf(stderr, "[Client] Failed to send ADDACCESS.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) printf("[Client] Access added.\n");
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] ADDACCESS failed (%d): %s\n", err.code, err.message); }
            else printf("[Client] Unexpected response (%d).\n", rh.command);
        } else if (strcmp(command, "REMACCESS") == 0) {
            // Accept both:
            //   REMACCESS <filename> <username>
            //   REMACCESS -R|-W <filename> <username>
            // The flag is ignored on the server (removal is role-agnostic),
            // but we parse it for user convenience to avoid mis-parsing filenames.
            char* t1 = strtok(NULL, " ");
            char* fname = NULL;
            char* target = NULL;
            if (!t1) { printf("[Client] Usage: REMACCESS [-R|-W] <filename> <username>\n"); continue; }
            if (t1[0] == '-' && (strcasecmp(t1, "-R") == 0 || strcasecmp(t1, "-W") == 0)) {
                fname = strtok(NULL, " ");
                target = strtok(NULL, " ");
            } else {
                fname = t1;
                target = strtok(NULL, " ");
            }
            if (!fname || !target) { printf("[Client] Usage: REMACCESS [-R|-W] <filename> <username>\n"); continue; }

            MsgAccessChange ac = (MsgAccessChange){0};
            strncpy(ac.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(ac.target, target, MAX_USERNAME_LEN-1);
            strncpy(ac.requester, username, MAX_USERNAME_LEN-1);
            ac.is_writer = 0; // ignored by server on removal
            MsgHeader h = { .command = CMD_REM_ACCESS, .payload_size = sizeof(ac) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &ac, sizeof(ac))) {
                LOGE_CLIENT("Failed to send REMACCESS.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) printf("[Client] Access removed.\n");
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] REMACCESS failed (%d): %s\n", err.code, err.message); }
            else printf("[Client] Unexpected response (%d).\n", rh.command);
        } else if (strcmp(command, "INFO") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: INFO <filename>\n"); continue; }
        // Additional command handling for CHECKPOINT, LISTCHECKPOINTS, VIEWCHECKPOINT, REVERT, etc.
            MsgInfoRequest rq = (MsgInfoRequest){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1);
            MsgHeader h = { .command = CMD_INFO, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                LOGE_CLIENT("Failed to send INFO.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_INFO_RESP && rh.payload_size == sizeof(MsgInfoResponse)) {
                MsgInfoResponse resp; if (recv_all(nm_socket, &resp, sizeof(resp))) { printf("%s\n", resp.info); }
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] INFO failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain unexpected
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t chunk = rem > sizeof(drain)?sizeof(drain):rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "CHECKPOINT") == 0) {
            char* fname = strtok(NULL, " "); char* tag = strtok(NULL, " ");
            if (!fname || !tag) { printf("[Client] Usage: CHECKPOINT <filename> <tag>\n"); continue; }
            MsgCheckpointCreate rq = {0}; strncpy(rq.filename, fname, MAX_FILENAME_LEN-1); strncpy(rq.tag, tag, sizeof(rq.tag)-1); strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_CHECKPOINT_CREATE, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))) { LOGE_CLIENT("Failed to send CHECKPOINT.\n"); continue; }
            MsgHeader rh; if (!recv_all(nm_socket,&rh,sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) { printf("[Client] Checkpoint '%s' created for '%s'.\n", tag, fname); }
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] CHECKPOINT failed (%d): %s\n", err.code, err.message); }
            else { size_t rem=rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response (%d).\n", rh.command); }
        } else if (strcmp(command, "LISTCHECKPOINTS") == 0) {
            char* fname = strtok(NULL, " "); if(!fname){ printf("[Client] Usage: LISTCHECKPOINTS <filename>\n"); continue; }
            MsgCheckpointList rq = {0}; strncpy(rq.filename, fname, MAX_FILENAME_LEN-1); strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_CHECKPOINT_LIST, .payload_size = sizeof(rq) };
            if(!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))){ LOGE_CLIENT("Failed to send LISTCHECKPOINTS.\n"); continue; }
            MsgHeader rh; if(!recv_all(nm_socket,&rh,sizeof(rh))){ LOGE_CLIENT("NM disconnected.\n"); break; }
            if(rh.command == CMD_CHECKPOINT_LIST_RESP && rh.payload_size == sizeof(MsgCheckpointListResponse)){
                MsgCheckpointListResponse resp={0}; if(!recv_all(nm_socket,&resp,sizeof(resp))){ LOGE_CLIENT("Failed to read LISTCHECKPOINTS payload.\n"); break; }
                if(resp.tags[0]=='\0') printf("[Client] No checkpoints.\n"); else printf("%s", resp.tags);
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] LISTCHECKPOINTS failed (%d): %s\n", err.code, err.message); }
            else { size_t rem=rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response (%d).\n", rh.command); }
        } else if (strcmp(command, "VIEWCHECKPOINT") == 0) {
            char* fname = strtok(NULL, " "); char* tag = strtok(NULL, " ");
            if(!fname || !tag){ printf("[Client] Usage: VIEWCHECKPOINT <filename> <tag>\n"); continue; }
            MsgCheckpointView rq = {0}; strncpy(rq.filename, fname, MAX_FILENAME_LEN-1); strncpy(rq.tag, tag, sizeof(rq.tag)-1); strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_CHECKPOINT_VIEW, .payload_size = sizeof(rq) };
            if(!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))){ LOGE_CLIENT("Failed to send VIEWCHECKPOINT.\n"); continue; }
            MsgHeader rh; if(!recv_all(nm_socket,&rh,sizeof(rh))){ LOGE_CLIENT("NM disconnected.\n"); break; }
            if(rh.command == CMD_ACK && rh.payload_size >= 0){ int n=rh.payload_size; if(n>0){ char* buf=malloc((size_t)n+1); if(!buf){ LOGE_CLIENT("OOM.\n"); continue; } if(!recv_all(nm_socket,buf,(size_t)n)){ LOGE_CLIENT("Failed reading checkpoint content.\n"); free(buf); continue; } buf[n]='\0'; fwrite(buf,1,(size_t)n,stdout); if(n>0 && buf[n-1] != '\n') putchar('\n'); free(buf);} else { /* empty snapshot */ printf("\n"); } }
            else if(rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] VIEWCHECKPOINT failed (%d): %s\n", err.code, err.message); }
            else { size_t rem=rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response (%d).\n", rh.command); }
        } else if (strcmp(command, "REVERT") == 0) {
            char* fname = strtok(NULL, " "); char* tag = strtok(NULL, " ");
            if(!fname || !tag){ printf("[Client] Usage: REVERT <filename> <tag>\n"); continue; }
            MsgCheckpointRevert rq = {0}; strncpy(rq.filename, fname, MAX_FILENAME_LEN-1); strncpy(rq.tag, tag, sizeof(rq.tag)-1); strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_CHECKPOINT_REVERT, .payload_size = sizeof(rq) };
            if(!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))){ LOGE_CLIENT("Failed to send REVERT.\n"); continue; }
            MsgHeader rh; if(!recv_all(nm_socket,&rh,sizeof(rh))){ LOGE_CLIENT("NM disconnected.\n"); break; }
            if(rh.command == CMD_ACK){ printf("[Client] Reverted '%s' to checkpoint '%s'.\n", fname, tag); }
            else if(rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] REVERT failed (%d): %s\n", err.code, err.message); }
            else { size_t rem=rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response (%d).\n", rh.command); }
        } else if (strcmp(command, "UNDO") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: UNDO <filename>\n"); continue; }
            MsgUndoRequest rq = (MsgUndoRequest){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_UNDO, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                LOGE_CLIENT("Failed to send UNDO.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) {
                printf("[Client] Undo applied to '%s'.\n", fname);
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err));
                printf("[Client] UNDO failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain unexpected response
                size_t rem = rh.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response to UNDO (%d).\n", rh.command);
            }
        } else if (strcmp(command, "READ") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: READ <filename>\n");
                continue;
            }

            // Ask NM for SS address owning this file
            MsgHeader h = (MsgHeader){0};
            MsgReadFile req = (MsgReadFile){0};
            h.command = CMD_READ_FILE;
            h.payload_size = sizeof(req);
            strncpy(req.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(req.requester, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &req, sizeof(req))) {
                LOGE_CLIENT("Failed to send READ to NM.\n");
                continue;
            }

            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                LOGE_CLIENT("No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }
            if (rh.command != CMD_READ_FILE_RESP || rh.payload_size != sizeof(MsgReadFileResponse)) {
                // Drain unexpected payload
                size_t rem = rh.payload_size; char drain[512];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response (%d).\n", rh.command);
                continue;
            }

            MsgReadFileResponse addr = {0};
            if (!recv_all(nm_socket, &addr, sizeof(addr))) {
                LOGE_CLIENT("Failed to read READ response payload.\n");
                break; // Treat as NM disconnect and exit
            }
            if (!addr.found || addr.ss_port <= 0 || addr.ss_ip[0] == '\0') {
                printf("[Client] READ failed (404): file not found.\n");
                continue;
            }

            // Connect directly to SS
            int ss_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (ss_fd < 0) { perror("[Client] socket"); continue; }
            struct sockaddr_in ss_addr; memset(&ss_addr, 0, sizeof(ss_addr));
            ss_addr.sin_family = AF_INET;
            ss_addr.sin_port = htons((uint16_t)addr.ss_port);
            if (inet_pton(AF_INET, addr.ss_ip, &ss_addr.sin_addr) <= 0) {
                perror("[Client] inet_pton"); close(ss_fd); continue;
            }
            if (connect(ss_fd, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
                perror("[Client] connect to SS"); close(ss_fd); continue;
            }
            // Header-based READ: send CMD_READ_FILE with requester for ACL
            {
                MsgReadFile rreq = (MsgReadFile){0};
                strncpy(rreq.filename, fname, MAX_FILENAME_LEN - 1);
                strncpy(rreq.requester, username, MAX_USERNAME_LEN - 1);
                MsgHeader rhdr = (MsgHeader){ .command = CMD_READ_FILE, .payload_size = sizeof(rreq) };
                if (!send_all(ss_fd, &rhdr, sizeof(rhdr)) || !send_all(ss_fd, &rreq, sizeof(rreq))) {
                    LOGE_CLIENT("Failed to send READ to SS.\n");
                    close(ss_fd); continue;
                }
                MsgHeader rresp;
                if (!recv_all(ss_fd, &rresp, sizeof(rresp))) {
                    LOGE_CLIENT("No response from SS.\n");
                    close(ss_fd); continue;
                }
                if (rresp.command == CMD_ERROR && rresp.payload_size == sizeof(MsgError)) {
                    MsgError err; if (recv_all(ss_fd, &err, sizeof(err))) {
                        printf("[Client] READ failed (%d): %s\n", err.code, err.message);
                    } else {
                        printf("[Client] READ failed (unknown error).\n");
                    }
                    close(ss_fd);
                    continue;
                }
                if (rresp.command != CMD_ACK || rresp.payload_size < 0) {
                    // Drain any unexpected payload and report
                    size_t rem = rresp.payload_size; char drain[512];
                    while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(ss_fd, drain, ch)) break; rem -= ch; }
                    printf("[Client] Unexpected response from SS (%d).\n", rresp.command);
                    close(ss_fd); continue;
                }
                int nbytes = rresp.payload_size;
                if (nbytes > 0) {
                    char* cbuf = (char*)malloc((size_t)nbytes + 1);
                    if (!cbuf) { LOGE_CLIENT("OOM.\n"); close(ss_fd); continue; }
                    if (!recv_all(ss_fd, cbuf, (size_t)nbytes)) {
                        LOGE_CLIENT("Failed to receive file content.\n");
                        free(cbuf); close(ss_fd); continue;
                    }
                    cbuf[nbytes] = '\0';
                    fwrite(cbuf, 1, (size_t)nbytes, stdout);
                    // Ensure a trailing newline for cleanliness
                    if (nbytes > 0 && cbuf[nbytes-1] != '\n') putchar('\n');
                    free(cbuf);
                } else {
                    // Empty file; still print a newline for UX
                    putchar('\n');
                }
                close(ss_fd);
            }
    } else if (strcmp(command, "STREAM") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: STREAM <filename>\n"); continue; }

            // Ask NM for SS address for this file
            MsgHeader h = (MsgHeader){0};
            MsgReadFile req = (MsgReadFile){0};
            h.command = CMD_READ_FILE; h.payload_size = sizeof(req);
            strncpy(req.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(req.requester, username, MAX_USERNAME_LEN - 1);
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &req, sizeof(req))) {
                LOGE_CLIENT("Failed to send STREAM routing query.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command != CMD_READ_FILE_RESP || rh.payload_size != sizeof(MsgReadFileResponse)) {
                // Drain unexpected
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if (!recv_all(nm_socket, drain, ch)) break; rem -= ch; }
                printf("[Client] Unexpected NM response to STREAM routing (%d).\n", rh.command);
                continue;
            }
            MsgReadFileResponse addr = {0}; if (!recv_all(nm_socket, &addr, sizeof(addr))) { LOGE_CLIENT("Failed reading routing payload.\n"); break; }
            if (!addr.found) { printf("[Client] STREAM failed: file not found.\n"); continue; }

            // Connect to Storage Server
            int ss_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (ss_fd < 0) { perror("[Client] socket"); continue; }
            struct sockaddr_in ss_addr; memset(&ss_addr, 0, sizeof(ss_addr));
            ss_addr.sin_family = AF_INET; ss_addr.sin_port = htons((uint16_t)addr.ss_port);
            if (inet_pton(AF_INET, addr.ss_ip, &ss_addr.sin_addr) <= 0) { perror("[Client] inet_pton"); close(ss_fd); continue; }
            if (connect(ss_fd, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) { perror("[Client] connect to SS"); close(ss_fd); continue; }

            // Request streaming via CMD_STREAM; SS replies with ACK(payload_size=total bytes).
            // Read exactly that many bytes and print as they arrive.
            MsgStreamRequest sr = (MsgStreamRequest){0};
            strncpy(sr.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(sr.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader sh = (MsgHeader){ .command = CMD_STREAM, .payload_size = sizeof(sr) };
            if (!send_all(ss_fd, &sh, sizeof(sh)) || !send_all(ss_fd, &sr, sizeof(sr))) {
                LOGE_CLIENT("STREAM: failed to send stream request.\n");
                close(ss_fd);
                continue;
            }
            // Read response header: ACK with size or ERROR
            MsgHeader stream_hdr;
            if (!recv_all(ss_fd, &stream_hdr, sizeof(stream_hdr))) {
                LOGE_CLIENT("STREAM: no response header from SS\n");
                close(ss_fd);
                continue;
            }
            
            if (stream_hdr.command == CMD_ERROR && stream_hdr.payload_size == (int)sizeof(MsgError)) {
                MsgError err;
                if (recv_all(ss_fd, &err, sizeof(err))) {
                    printf("STREAM failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("STREAM failed: unknown error from Storage Server\n");
                }
                close(ss_fd);
                continue;
            }
            
            if (stream_hdr.command != CMD_ACK) {
                printf("STREAM: unexpected response (%d)\n", stream_hdr.command);
                close(ss_fd);
                continue;
            }
            
            // ACK payload_size tells us exactly how many bytes to read
            size_t total_bytes = (size_t)stream_hdr.payload_size;
            size_t received = 0;
            
            // Unbuffer stdout to see live output
            setvbuf(stdout, NULL, _IONBF, 0);

            char rbuf[1024];
            while (received < total_bytes) {
                size_t to_read = total_bytes - received;
                if (to_read > sizeof(rbuf)) to_read = sizeof(rbuf);
                ssize_t n = recv(ss_fd, rbuf, to_read, 0);
                if (n <= 0) {
                    printf("\nError: Storage Server disconnected during streaming\n");
                    break;
                }
                fwrite(rbuf, 1, (size_t)n, stdout);
                received += (size_t)n;
            }
            
            // Ensure output ends with newline for clean prompt
            if (received > 0 && received == total_bytes) {
                // Check if last char was newline; if not, add one
                // (we'd need to track last byte, but simpler: always ensure newline)
                fputc('\n', stdout);
            }
            // Restore stdout buffering to a sane default for the interactive prompt
            {
                int tty = isatty(fileno(stdout));
                setvbuf(stdout, NULL, tty ? _IOLBF : _IOFBF, 0);
            }
            close(ss_fd);
        } else if (strcmp(command, "LIST") == 0) {
            // Request user list from Name Server
            MsgHeader h = (MsgHeader){ .command = CMD_LIST_USERS, .payload_size = 0 };
            if (!send_all(nm_socket, &h, sizeof(h))) { LOGE_CLIENT("Failed to send LIST.\n"); continue; }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_LIST_USERS_RESP && rh.payload_size == sizeof(MsgUsersListResponse)) {
                MsgUsersListResponse resp = {0};
                if (!recv_all(nm_socket, &resp, sizeof(resp))) { LOGE_CLIENT("Failed to read LIST payload.\n"); break; }
                if (resp.users[0] == '\0') { printf("[Client] No users currently registered.\n"); }
                else { printf("%s", resp.users); }
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] LIST failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain any unexpected payload
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if (!recv_all(nm_socket, drain, ch)) break; rem -= ch; }
                printf("[Client] Unexpected response to LIST (%d).\n", rh.command);
            }
        } else if (strcmp(command, "EXEC") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: EXEC <filename>\n"); continue; }
            MsgExecRequest rq = (MsgExecRequest){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_EXEC, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                LOGE_CLIENT("Failed to send EXEC.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK && rh.payload_size >= 0) {
                int n = rh.payload_size; if (n > 0) {
                    char* buf = (char*)malloc((size_t)n+1); if (!buf) { LOGE_CLIENT("OOM.\n"); continue; }
                    if (!recv_all(nm_socket, buf, (size_t)n)) { LOGE_CLIENT("Failed to read EXEC output.\n"); free(buf); continue; }
                    buf[n] = '\0';
                    fwrite(buf, 1, (size_t)n, stdout);
                    // Ensure trailing newline for cleanliness if output didn't end with one
                    if (n > 0 && buf[n-1] != '\n') putchar('\n');
                    free(buf);
                } else {
                    // No output
                }
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] EXEC failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if (!recv_all(nm_socket, drain, ch)) break; rem -= ch; }
                printf("[Client] Unexpected response to EXEC (%d).\n", rh.command);
            }
        } else if (strcmp(command, "REQUESTACCESS") == 0) {
            // REQUESTACCESS -R|-W <filename>
            char* flag = strtok(NULL, " ");
            char* fname = strtok(NULL, " ");
            if (!flag || !fname || !(strcasecmp(flag, "-R")==0 || strcasecmp(flag, "-W")==0)) {
                printf("[Client] Usage: REQUESTACCESS -R|-W <filename>\n");
                continue;
            }
            int want_write = (strcasecmp(flag, "-W") == 0);
            MsgAccessRequestCreate rq = (MsgAccessRequestCreate){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1);
            rq.want_write = want_write;
            strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_REQUEST_ACCESS, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))) { LOGE_CLIENT("Failed to send REQUESTACCESS.\n"); continue; }
            MsgHeader rh; if(!recv_all(nm_socket,&rh,sizeof(rh))) { LOGE_CLIENT("NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) printf("[Client] Access request submitted.\n");
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] REQUESTACCESS failed (%d): %s\n", err.code, err.message); }
            else { // drain
                size_t rem = rh.payload_size; char drain[256]; while(rem>0){ size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; }
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "LISTREQUESTS") == 0) {
            // LISTREQUESTS <filename>
            char* fname = strtok(NULL, " "); if(!fname){ printf("[Client] Usage: LISTREQUESTS <filename>\n"); continue; }
            MsgAccessRequestList rq = (MsgAccessRequestList){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1); strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_LIST_REQUESTS, .payload_size = sizeof(rq) };
            if(!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))){ LOGE_CLIENT("Failed to send LISTREQUESTS.\n"); continue; }
            MsgHeader rh; if(!recv_all(nm_socket,&rh,sizeof(rh))){ LOGE_CLIENT("NM disconnected.\n"); break; }
            if(rh.command == CMD_LIST_REQUESTS_RESP && rh.payload_size == sizeof(MsgAccessRequestListResp)){
                MsgAccessRequestListResp resp={0}; if(!recv_all(nm_socket,&resp,sizeof(resp))){ LOGE_CLIENT("Failed reading LISTREQUESTS payload.\n"); break; }
                if(resp.requests[0]=='\0') printf("[Client] No pending requests.\n"); else printf("%s", resp.requests);
            } else if(rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] LISTREQUESTS failed (%d): %s\n", err.code, err.message); }
            else { size_t rem = rh.payload_size; char drain[256]; while(rem>0){ size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response (%d).\n", rh.command); }
        } else if (strcmp(command, "APPROVEREQUEST") == 0 || strcmp(command, "DENYREQUEST") == 0) {
            int approving = (strcmp(command, "APPROVEREQUEST") == 0);
            // APPROVEREQUEST -R|-W <filename> <username>
            // DENYREQUEST <filename> <username>
            char* flag = NULL; char* fname = NULL; char* target = NULL; int grant_write = 0; int parse_ok = 0;
            if (approving) {
                flag = strtok(NULL, " "); fname = strtok(NULL, " "); target = strtok(NULL, " ");
                if(flag && fname && target && (strcasecmp(flag,"-R")==0 || strcasecmp(flag,"-W")==0)) { grant_write = (strcasecmp(flag,"-W")==0); parse_ok = 1; }
            } else {
                fname = strtok(NULL, " "); target = strtok(NULL, " "); if(fname && target){ parse_ok = 1; }
            }
            if(!parse_ok){
                if(approving) printf("[Client] Usage: APPROVEREQUEST -R|-W <filename> <username>\n"); else printf("[Client] Usage: DENYREQUEST <filename> <username>\n");
                continue;
            }
            MsgAccessRequestRespond rq = (MsgAccessRequestRespond){0};
            strncpy(rq.filename,fname,MAX_FILENAME_LEN-1); strncpy(rq.target,target,MAX_USERNAME_LEN-1);
            rq.approve = approving ? 1 : 0; rq.grant_write = approving ? grant_write : 0; strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_RESPOND_REQUEST, .payload_size = sizeof(rq) };
            if(!send_all(nm_socket,&h,sizeof(h)) || !send_all(nm_socket,&rq,sizeof(rq))){ LOGE_CLIENT("Failed to send RESPONDREQUEST.\n"); continue; }
            MsgHeader rh; if(!recv_all(nm_socket,&rh,sizeof(rh))){ LOGE_CLIENT("NM disconnected.\n"); break; }
            if(rh.command == CMD_ACK){ if(approving) printf("[Client] Request approved.\n"); else printf("[Client] Request denied.\n"); }
            else if(rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket,&err,sizeof(err)); printf("[Client] %s failed (%d): %s\n", approving?"APPROVEREQUEST":"DENYREQUEST", err.code, err.message); }
            else { size_t rem=rh.payload_size; char drain[256]; while(rem>0){ size_t ch=rem>sizeof(drain)?sizeof(drain):rem; if(!recv_all(nm_socket,drain,ch)) break; rem -= ch; } printf("[Client] Unexpected response (%d).\n", rh.command); }
        } else {
            printf("[Client] Unknown command: %s\n", command);
        }
    }

    printf("[Client] Disconnecting...\n");
    close(nm_socket);
    return 0;
}
