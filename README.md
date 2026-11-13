# C++ High-Performance Multi-Threaded HTTP Server (v0.7)

🏫 Project Information (COME 361 - Operating Systems Concepts)

-Group Name: WEB O

-Team Members:

-Ziya Gürkan Çendek (202303001061)

-Ziya Anıl Alpaslan (202303001064)

-Muhammed Ali Esen (202303001136)

📂 Module Breakdown & Responsibilities

1.Network & HTTP Layer: TCP socket management, non-blocking I/O, and cross-platform Reactor (epoll / kqueue) architecture.

2.Thread Pool & Synchronization: Work-stealing scheduler, mutex & condition_variable synchronization, concurrent request handling, and graceful shutdown.

3.File Server, Logging & Benchmark: Thread-safe logging system, custom slab memory pool (MemoryPool), CLI hardware dashboard, and concurrency benchmarking.

This project implements a high-concurrency, modern HTTP server architecture in C++17 utilizing the Reactor Pattern (Non-blocking I/O), a Work-Stealing Thread Pool, a Custom Slab-Style Memory Pool, a Thread-Safe Logging System, and a Real-Time Terminal Hardware CLI Dashboard.
---

## 🚀 Key Features & Architecture

### 1. 🖥️ Real-Time Terminal CLI Dashboard
An independent background monitoring thread (`run_cli_dashboard`) samples system metrics every second and visualizes them on the terminal using ANSI colors and progress bars:
- **CPU Utilization:** Instant process CPU load (%) and active worker thread count.
- **RAM / Memory Tracking:** Process physical memory footprint (RSS - Resident Set Size), total host system RAM, and live Custom Memory Pool allocation state (`used / total blocks`).
- **Storage / Disk Metrics:** Filesystem storage capacity, used space, and utilization percentage via `statvfs`.
- **Work-Stealing & Balancing:** Local tasks completed vs. stolen tasks acquired from peer worker queues.
- **Network & Throughput:** Total processed requests, real-time throughput in **Requests Per Second (RPS)**, and total bytes sent.

### **2. 📝 Thread-Safe Logging System (Logger)**

- **Singleton Architecture:** Managed globally via `Logger::get_instance()` to ensure centralized logging across all subsystems.
- **Thread Safety:** Protected by `std::mutex` to prevent interleaved writes and data races during high-concurrency operations.
- **Log Levels:** Supports `DEBUG`, `INFO`, `WARNING`, and `LOG_ERROR` severity levels through helper macros (`LOG_INFO`, `LOG_ERROR`, etc.).
- **Persistent Output & Timestamps:** Writes millisecond-precision timestamps (`YYYY-MM-DD HH:MM:SS.mmm`) directly to `server.log`.

### 3. ⚡ Reactor Pattern & Non-Blocking I/O (`epoll` / `kqueue`)
- **Non-Blocking Sockets:** All listening and connection descriptors are configured via `fcntl(fd, F_SETFL, O_NONBLOCK)` to prevent thread blocking.
- **Cross-Platform Event Demultiplexing:**
  - **Linux:** Native `epoll` (`epoll_create1`, `epoll_ctl`, `epoll_wait` with Edge-Triggered `EPOLLET`).
  - **macOS / BSD:** Native `kqueue` and `kevent`.
- **TCP Graceful Shutdown:** Employs `shutdown(client_fd, SHUT_WR)` to deliver a clean `FIN` packet, drains trailing bytes, and closes the connection without triggering connection reset (`RST` / Errno 54) errors.
- **Signal Handling:** Disables broken pipe signals with `signal(SIGPIPE, SIG_IGN)`.

### 4. 🧵 Work-Stealing Thread Pool & Load Balancing
- **Worker Thread Pool:** Spawns a pool of worker threads sized to match available hardware cores (`std::thread::hardware_concurrency()`).
- **Load Balancing (Round-Robin):** Incoming accepted connections are assigned evenly across worker local task queues (`std::deque`).
- **Work-Stealing:** When a worker exhausts its own queue, it attempts to steal tasks from the back (`pop_back()`) of busy peer queues, eliminating CPU idle time and mitigating workload skew.

### 5. 🧠 Memory Management: Custom Slab-Style Memory Pool
- **Pre-Allocation:** Allocates a contiguous block of memory during startup (512 blocks × 8 KB = 4 MB buffer pool).
- **Zero-Overhead Allocations ($O(1)$):** Client requests obtain reusable buffer blocks from an internal free-list without invoking dynamic `malloc` / `new` calls on the heap.
- **RAII Resource Management:** The `PooledBuffer` wrapper guarantees that buffers are returned to the pool immediately upon request completion or unexpected disconnection, completely preventing memory leaks and heap fragmentation.

---

## ⚙️ Compilation

### Linux & macOS
Compile using a C++17 compliant compiler with POSIX thread support:

```bash
# Using Clang++
clang++ -std=c++17 -Wall -Wextra -pthread main.cpp -o server

# Or using G++
g++ -std=c++17 -Wall -Wextra -pthread main.cpp -o server
```

---

## 🏃‍♂️ Running and Testing

1. **Start the Server:**
```bash
./server
```

You will see the live terminal dashboard initialize:

```text
╔══════════════════════════════════════════════════════════════════════════╗
║     ⚡ HIGH-PERFORMANCE MULTI-THREADED HTTP SERVER LIVE DASHBOARD ⚡     ║
╚══════════════════════════════════════════════════════════════════════════╝
 ● Port: 8080  |  ● Uptime: 00:01:24  |  ● Active Connections: 0

[CPU & THREAD POOL]
  Process CPU:  [=====>                  ] 21.8 %
  Worker Threads: 8 active worker cores

[RAM & MEMORY MANAGEMENT]
  Process RSS:  8.15 MB (Total System RAM: 16.00 GB)
  Memory Pool:  [===>                    ] 14/512 blocks (2.7 %)
  Pool Reserve: 4096 KB Pre-allocated Slab Pool

[STORAGE / DISK USAGE]
  Disk Space:   [===============>        ] 284.2 GB / 465.6 GB (61.0 %)

[LOAD BALANCING & WORK-STEALING]
  Local Tasks:  9840
  Stolen Tasks: 160 (Queue rebalancing)

[NETWORK & HTTP TRAFFIC]
  Total Requests: 10000 requests
  Throughput:     1850.4 req/sec (RPS)
  Data Sent:      2.48 MB
──────────────────────────────────────────────────────────────────────────
 Press Ctrl+C to stop the server.
```

2. **Run Concurrency & Benchmark Tests:**
Open a separate terminal window and benchmark the server using ApacheBench:
```bash
ab -n 10000 -c 100 http://127.0.0.1:8080/
```
Or test a simple request using `curl`:
```bash
curl -i http://localhost:8080/
```
