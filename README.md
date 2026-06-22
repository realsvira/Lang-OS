# Distributed File Collaboration System

A robust, high-performance distributed file collaboration system enabling real-time collaborative editing across multiple storage nodes.

## Key Features
- **Distributed Architecture:** Name Server (NM) for routing and metadata, Storage Servers (SS) for file storage and operations, and an interactive Client CLI.
- **Concurrent Editing:** Sentence-level locking enables multiple users to edit different parts of a file simultaneously without conflicts.
- **Access Control (ACL):** Fine-grained permissions (Owner, Writer, Reader) with persistent metadata tracking.
- **Versioning & Safety:** Snapshot-based UNDO, named CHECKPOINTs, and a Trash Bin for safe deletion and recovery.
- **Advanced Operations:** Live STREAM of file contents and EXEC support for executing stored scripts.
- **Performance Optimizations:** O(1) file lookup using hashing, smart caching in the Name Server, and efficient binary protocol communication.

---

## System Architecture

The system is composed of three main components:

- **Name Server (NM)**
  - Maintains a global registry of files and users
  - Routes client requests to appropriate storage servers
  - Uses hash-based indexing for O(1) file lookup
  - Caches metadata to reduce network overhead

- **Storage Server (SS)**
  - Stores file content and metadata
  - Enforces access control and concurrency rules
  - Manages checkpoints, undo history, and trash recovery
  - Handles sentence-level locking for concurrent writes

- **Client CLI**
  - Provides an interactive REPL interface
  - Sends commands to Name Server and Storage Servers
  - Displays outputs and logs in real time

---

## Core Design Decisions

- **Custom Binary Protocol**
  - Size-prefixed messages to avoid delimiter/sentinel issues
  - Reliable transmission of arbitrary file content

- **Hash-Based File Registry**
  - FNV-1a hashing with fixed buckets for constant-time lookup

- **Sentence-Level Locking**
  - Files split using delimiters (`.`, `!`, `?`)
  - Fine-grained locks allow parallel edits on different sentences

- **Persistent Metadata**
  - Each file maintains owner, timestamps, and ACL lists
  - Metadata stored alongside file content for consistency

---

## Quick Start

```bash
make
./name_server
./storage_server 9001
./client
```

Order: Name Server → Storage Server(s) → Client

## Example Usage

```bash
CREATE report.txt
WRITE report.txt 0
READ report.txt
ADDACCESS -R report.txt bob
```

## Contributions

**Kavish (GitHub: [Parzival-07](https://github.com/Parzival-07))**

- Architected the overall distributed system including Name Server, Storage Server, and Client CLI interaction model.
- Designed and implemented the custom size-prefixed binary protocol for reliable inter-component communication.
- Built the O(1) file routing mechanism using hash-based indexing in the Name Server.
- Developed the sentence-level locking system to enable safe concurrent edits across clients.
- Implemented access control (ACL), including reader/writer roles and enforcement across operations.
- Engineered snapshot-based versioning (UNDO and CHECKPOINT) and the trash-based recovery mechanism.
- Contributed to integration logic between components and ensured consistency across distributed operations.

## Limitations

- Single Name Server (no replication or failover)
- No authentication (username-based identification only)
- No data replication across storage servers
- Single-level undo support
- POSIX-dependent implementation
