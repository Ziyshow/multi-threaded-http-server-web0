# C++ Custom HTTP Server (v0.1)

This project was developed within the scope of an Operating Systems course to learn and apply low-level network programming, sockets, and system calls. The project starts with a basic **Single-Threaded and Blocking** HTTP server architecture (`v0.1`) and is designed to be **cross-platform**, supporting both Unix/Linux/macOS and Windows environments.

---

## 🚀 Features (v0.1)

* **Raw Socket Management:** Establishes a communication channel over IPv4 and TCP using standard socket APIs (POSIX on Unix, Winsock on Windows).
* **Port Binding and Protection:** Prevents "Address already in use" errors using `setsockopt()` (`SO_REUSEADDR`) and binds the server to a specified port.
* **Connection Queue:** Creates a waiting queue (backlog) for incoming TCP connection requests using `listen()`.
* **Client Acceptance and Separation:** Allocates a dedicated new socket (`client_fd`) for each client using `accept()` without blocking the main listening socket.
* **HTTP Request Reading and Responding:** Reads raw HTTP requests and returns a static HTML response in standard HTTP/1.1 format (`200 OK`, `Content-Type`, `Content-Length`).
* **Resource Cleanup:** Safely closes file descriptors and socket handles.

---

## 🛠️ Requirements & Cross-Platform Support

The server can run on any major operating system:
* **Compiler:** A C++17 compliant compiler (`g++`, `clang++`, or MSVC).
* **Operating Systems:** 
  * Linux / macOS (Native POSIX sockets)
  * Windows 10/11 (via MinGW/g++ or MSVC with Winsock linkage)

---

## ⚙️ Compilation Instructions

### 🐧 Linux / macOS (Unix)
Run the following command in your terminal:
```bash
clang++ -std=c++17 -Wall -Wextra main.cpp -o server
```

### 🪟 Windows
Depending on your Windows development toolchain:

#### Option A: Using MinGW (g++)
If you use MinGW (GCC for Windows), compile with the Winsock library linked (`-lws2_32`):
```bash
g++ -std=c++17 -Wall -Wextra main.cpp -o server.exe -lws2_32
```

#### Option B: Using Visual Studio (MSVC)
Open your Developer Command Prompt for VS and run:
```cmd
cl /std:c++17 /EHsc main.cpp /Fe:server.exe ws2_32.lib
```

---

## 🚀 Running and Testing

1. Start the compiled server:
   * **Unix:** `./server`
   * **Windows:** `server.exe`
2. Open your web browser and navigate to: `http://localhost:8080`
3. Alternatively, test via terminal using `curl`:
   ```bash
   curl http://localhost:8080
   ```

---

## 🗺️ Roadmap (Future Versions)
* **[v0.1]** – Single-threaded blocking basic HTTP server with cross-platform support (Current)
* **[v0.2]** – **Thread-per-Connection** architecture using `std::thread` for concurrent requests
* **[v1.0]** – **Thread Pool** and Synchronization architecture (Mutex / Queue) for efficient resource management.