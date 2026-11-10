#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <memory>
#include <chrono>
#include <cstring>
#include <cerrno>
#include <csignal>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/statvfs.h>

// Platform tespiti
#if defined(__linux__)
    #include <sys/epoll.h>
    #include <sys/sysinfo.h>
    #define USE_EPOLL 1
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
    #include <sys/event.h>
    #include <sys/time.h>
    #include <mach/mach.h>
    #include <sys/sysctl.h>
    #define USE_KQUEUE 1
#else
    #error "Bu sistem ne epoll ne de kqueue destekliyor."
#endif

constexpr int PORT = 8080;
constexpr int MAX_EVENTS = 256;
constexpr size_t POOL_BLOCK_SIZE = 8192; // 8 KB sabit buffer boyutu
constexpr size_t POOL_BLOCK_COUNT = 512; // 512 blok (4 MB önceden ayrılmış alan)

// =============================================================================
// GLOBAL METRİKLER (Server Statistics)
// =============================================================================
struct ServerMetrics {
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::chrono::steady_clock::time_point start_time{std::chrono::steady_clock::now()};
};

static ServerMetrics g_metrics;

// =============================================================================
// 1. BELLEK YÖNETİMİ: Sabit Bloklu Custom Memory Pool (Slab-Style)
// =============================================================================
class MemoryPool {
public:
    MemoryPool(size_t block_size, size_t block_count)
        : block_size_(block_size), block_count_(block_count) {
        
        raw_memory_ = new char[block_size_ * block_count_];
        for (size_t i = 0; i < block_count_; ++i) {
            free_list_.push_back(raw_memory_ + (i * block_size_));
        }
    }

    ~MemoryPool() {
        delete[] raw_memory_;
    }

    char* allocate() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        if (free_list_.empty()) {
            fallback_allocations_++;
            return new char[block_size_];
        }
        char* block = free_list_.back();
        free_list_.pop_back();
        used_blocks_++;
        return block;
    }

    void deallocate(char* block) {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        if (block >= raw_memory_ && block < raw_memory_ + (block_size_ * block_count_)) {
            free_list_.push_back(block);
            if (used_blocks_ > 0) used_blocks_--;
        } else {
            delete[] block;
            if (fallback_allocations_ > 0) fallback_allocations_--;
        }
    }

    size_t get_block_size() const { return block_size_; }
    size_t get_total_blocks() const { return block_count_; }
    size_t get_used_blocks() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        return used_blocks_;
    }
    size_t get_fallback_count() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        return fallback_allocations_;
    }

private:
    size_t block_size_;
    size_t block_count_;
    char* raw_memory_;
    std::vector<char*> free_list_;
    std::mutex pool_mutex_;
    size_t used_blocks_ = 0;
    size_t fallback_allocations_ = 0;
};

class PooledBuffer {
public:
    PooledBuffer(MemoryPool& pool) : pool_(pool), data_(pool.allocate()) {}
    ~PooledBuffer() {
        if (data_) pool_.deallocate(data_);
    }

    PooledBuffer(const PooledBuffer&) = delete;
    PooledBuffer& operator=(const PooledBuffer&) = delete;
    PooledBuffer(PooledBuffer&& other) noexcept : pool_(other.pool_), data_(other.data_) {
        other.data_ = nullptr;
    }

    char* get() { return data_; }
    size_t capacity() const { return pool_.get_block_size(); }

private:
    MemoryPool& pool_;
    char* data_;
};

// =============================================================================
// 2. WORK-STEALING THREAD POOL & LOAD BALANCING
// =============================================================================
class WorkStealingThreadPool {
public:
    using Task = std::function<void()>;

    explicit WorkStealingThreadPool(size_t num_threads)
        : queues_(num_threads), stop_(false), round_robin_counter_(0) {
        
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this, i]() {
                this->worker_loop(i);
            });
        }
    }

    ~WorkStealingThreadPool() {
        stop_ = true;
        cv_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
    }

    void submit(Task task) {
        size_t target_worker = round_robin_counter_++ % queues_.size();
        {
            std::lock_guard<std::mutex> lock(queues_[target_worker].mutex);
            queues_[target_worker].tasks.push_back(std::move(task));
        }
        cv_.notify_all();
    }

    size_t get_worker_count() const { return workers_.size(); }
    uint64_t get_local_tasks() const { return local_tasks_completed_.load(); }
    uint64_t get_stolen_tasks() const { return stolen_tasks_completed_.load(); }

private:
    struct WorkerQueue {
        std::deque<Task> tasks;
        std::mutex mutex;
    };

    void worker_loop(size_t my_id) {
        while (!stop_) {
            Task task;
            bool found_task = false;
            bool was_stolen = false;

            // 1. Kendi yerel kuyruğundan al
            {
                std::lock_guard<std::mutex> lock(queues_[my_id].mutex);
                if (!queues_[my_id].tasks.empty()) {
                    task = std::move(queues_[my_id].tasks.front());
                    queues_[my_id].tasks.pop_front();
                    found_task = true;
                }
            }

            // 2. İş yoksa diğer worker'ların kuyruğundan iş çal (Work-Stealing)
            if (!found_task) {
                size_t num_queues = queues_.size();
                for (size_t offset = 1; offset < num_queues; ++offset) {
                    size_t victim_id = (my_id + offset) % num_queues;
                    std::unique_lock<std::mutex> lock(queues_[victim_id].mutex, std::try_to_lock);
                    if (lock.owns_lock() && !queues_[victim_id].tasks.empty()) {
                        task = std::move(queues_[victim_id].tasks.back());
                        queues_[victim_id].tasks.pop_back();
                        found_task = true;
                        was_stolen = true;
                        break;
                    }
                }
            }

            if (found_task) {
                task();
                if (was_stolen) stolen_tasks_completed_++;
                else local_tasks_completed_++;
            } else {
                std::unique_lock<std::mutex> cv_lock(notify_mutex_);
                cv_.wait_for(cv_lock, std::chrono::milliseconds(5), [this]() {
                    return stop_.load();
                });
            }
        }
    }

    std::vector<std::thread> workers_;
    std::vector<WorkerQueue> queues_;
    std::atomic<bool> stop_;
    std::atomic<size_t> round_robin_counter_;
    std::mutex notify_mutex_;
    std::condition_variable cv_;
    std::atomic<uint64_t> local_tasks_completed_{0};
    std::atomic<uint64_t> stolen_tasks_completed_{0};
};

// =============================================================================
// 3. NON-BLOCKING I/O & EVENT DEMULTIPLEXER (epoll & kqueue Reactor)
// =============================================================================
bool set_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

class EventReactor {
public:
    EventReactor() {
#if defined(USE_EPOLL)
        poll_fd_ = epoll_create1(0);
#elif defined(USE_KQUEUE)
        poll_fd_ = kqueue();
#endif
    }

    ~EventReactor() {
        if (poll_fd_ >= 0) close(poll_fd_);
    }

    bool add_socket(int fd) {
#if defined(USE_EPOLL)
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd;
        return epoll_ctl(poll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0;
#elif defined(USE_KQUEUE)
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
        return kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0;
#endif
    }

    bool remove_socket(int fd) {
#if defined(USE_EPOLL)
        return epoll_ctl(poll_fd_, EPOLL_CTL_DEL, fd, nullptr) == 0;
#elif defined(USE_KQUEUE)
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        return kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0;
#endif
    }

    std::vector<int> poll_events(int timeout_ms = 1000) {
        std::vector<int> ready_fds;
#if defined(USE_EPOLL)
        struct epoll_event events[MAX_EVENTS];
        int nfds = epoll_wait(poll_fd_, events, MAX_EVENTS, timeout_ms);
        for (int i = 0; i < nfds; ++i) {
            ready_fds.push_back(events[i].data.fd);
        }
#elif defined(USE_KQUEUE)
        struct kevent events[MAX_EVENTS];
        struct timespec ts;
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000;
        int nfds = kevent(poll_fd_, nullptr, 0, events, MAX_EVENTS, &ts);
        for (int i = 0; i < nfds; ++i) {
            ready_fds.push_back(static_cast<int>(events[i].ident));
        }
#endif
        return ready_fds;
    }

private:
    int poll_fd_ = -1;
};

// =============================================================================
// 4. HTTP İSTEK İŞLEYİCİ
// =============================================================================
void handle_client(int client_fd, MemoryPool& memory_pool) {
    g_metrics.active_connections++;
    PooledBuffer buffer(memory_pool);

    ssize_t total_bytes_read = 0;
    while (true) {
        ssize_t bytes = read(client_fd, buffer.get() + total_bytes_read, buffer.capacity() - total_bytes_read - 1);
        if (bytes > 0) {
            total_bytes_read += bytes;
            buffer.get()[total_bytes_read] = '\0';
            if (strstr(buffer.get(), "\r\n\r\n") != nullptr) {
                break;
            }
        } else if (bytes == 0) {
            break;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd;
                pfd.fd = client_fd;
                pfd.events = POLLIN;
                int ret = poll(&pfd, 1, 100);
                if (ret > 0 && (pfd.revents & POLLIN)) {
                    continue;
                }
                break;
            }
            break;
        }
    }

    if (total_bytes_read > 0) {
        g_metrics.total_requests++;

        const char response_headers[] = 
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html; charset=UTF-8\r\n"
            "Content-Length: 104\r\n"
            "Connection: close\r\n"
            "\r\n"
            "<!DOCTYPE html><html><body><h1>200 OK</h1><p>Multi-threaded Server Calisiyor.</p></body></html>";
        
        size_t to_send = sizeof(response_headers) - 1;
        size_t total_sent = 0;

        while (total_sent < to_send) {
            ssize_t sent = write(client_fd, response_headers + total_sent, to_send - total_sent);
            if (sent > 0) {
                total_sent += sent;
            } else if (sent < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    struct pollfd pfd;
                    pfd.fd = client_fd;
                    pfd.events = POLLOUT;
                    poll(&pfd, 1, 50);
                    continue;
                }
                break;
            } else {
                break;
            }
        }
        g_metrics.bytes_sent += total_sent;
    }

    shutdown(client_fd, SHUT_WR);
    char drain[256];
    while (read(client_fd, drain, sizeof(drain)) > 0) {}
    close(client_fd);
    g_metrics.active_connections--;
}

// =============================================================================
// 5. CANLI DONANIM & SİSTEM İZLEME (CLI Dashboard)
// =============================================================================
struct SystemStats {
    double cpu_usage_pct = 0.0;
    size_t memory_rss_bytes = 0;
    size_t system_total_ram_bytes = 0;
    size_t system_free_ram_bytes = 0;
    uint64_t disk_total_bytes = 0;
    uint64_t disk_used_bytes = 0;
};

// Donanım metriklerini toplayan yardımcı sınıf
class HardwareSampler {
public:
    HardwareSampler() {
        last_sample_time_ = std::chrono::steady_clock::now();
        last_cpu_time_ = get_process_cpu_time_ms();
    }

    SystemStats sample() {
        SystemStats stats;

        // 1. CPU Kullanımı Hesaplama
        auto now = std::chrono::steady_clock::now();
        double wall_time_ms = std::chrono::duration<double, std::milli>(now - last_sample_time_).count();
        double current_cpu_time = get_process_cpu_time_ms();
        double delta_cpu_ms = current_cpu_time - last_cpu_time_;

        if (wall_time_ms > 0.0) {
            unsigned int cores = std::thread::hardware_concurrency();
            if (cores == 0) cores = 1;
            stats.cpu_usage_pct = (delta_cpu_ms / wall_time_ms) * 100.0;
            if (stats.cpu_usage_pct < 0.0) stats.cpu_usage_pct = 0.0;
        }

        last_sample_time_ = now;
        last_cpu_time_ = current_cpu_time;

        // 2. RAM (Bellek) Kullanımı
#if defined(__APPLE__)
        mach_task_basic_info info;
        mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
        if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
            stats.memory_rss_bytes = info.resident_size;
        }

        int mib[2] = {CTL_HW, HW_MEMSIZE};
        int64_t total_ram = 0;
        size_t len = sizeof(total_ram);
        if (sysctl(mib, 2, &total_ram, &len, nullptr, 0) == 0) {
            stats.system_total_ram_bytes = total_ram;
        }
#elif defined(__linux__)
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            stats.memory_rss_bytes = usage.ru_maxrss * 1024;
        }
        struct sysinfo si;
        if (sysinfo(&si) == 0) {
            stats.system_total_ram_bytes = si.totalram * si.mem_unit;
            stats.system_free_ram_bytes = si.freeram * si.mem_unit;
        }
#endif

        // 3. Storage (Disk Alanı) Kullanımı
        struct statvfs vfs;
        if (statvfs(".", &vfs) == 0) {
            stats.disk_total_bytes = static_cast<uint64_t>(vfs.f_blocks) * vfs.f_frsize;
            uint64_t disk_free = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize;
            stats.disk_used_bytes = stats.disk_total_bytes - disk_free;
        }

        return stats;
    }

private:
    std::chrono::steady_clock::time_point last_sample_time_;
    double last_cpu_time_ = 0.0;

    double get_process_cpu_time_ms() {
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            double utime = usage.ru_utime.tv_sec * 1000.0 + usage.ru_utime.tv_usec / 1000.0;
            double stime = usage.ru_stime.tv_sec * 1000.0 + usage.ru_stime.tv_usec / 1000.0;
            return utime + stime;
        }
        return 0.0;
    }
};

// ANSI İlerleme Çubuğu Çizen Yardımcı
std::string render_progress_bar(double pct, int bar_width = 24) {
    if (pct < 0.0) pct = 0.0;
    if (pct > 100.0) pct = 100.0;

    int filled = static_cast<int>((pct / 100.0) * bar_width);
    std::string bar = "[";
    for (int i = 0; i < bar_width; ++i) {
        if (i < filled) bar += "=";
        else if (i == filled) bar += ">";
        else bar += " ";
    }
    bar += "]";
    return bar;
}

// Canlı Terminal Dashboard Döngüsü
void run_cli_dashboard(MemoryPool& mem_pool, WorkStealingThreadPool& thread_pool, std::atomic<bool>& running) {
    HardwareSampler sampler;
    uint64_t last_requests = 0;
    auto last_time = std::chrono::steady_clock::now();

    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if (!running.load()) break;

        SystemStats stats = sampler.sample();

        auto now = std::chrono::steady_clock::now();
        double elapsed_sec = std::chrono::duration<double>(now - last_time).count();
        uint64_t current_requests = g_metrics.total_requests.load();
        double rps = (elapsed_sec > 0.0) ? (current_requests - last_requests) / elapsed_sec : 0.0;
        last_requests = current_requests;
        last_time = now;

        auto total_uptime_sec = std::chrono::duration_cast<std::chrono::seconds>(now - g_metrics.start_time).count();
        int uptime_h = total_uptime_sec / 3600;
        int uptime_m = (total_uptime_sec % 3600) / 60;
        int uptime_s = total_uptime_sec % 60;

        // Bellek Havuzu İstatistikleri
        size_t used_pool_blocks = mem_pool.get_used_blocks();
        size_t total_pool_blocks = mem_pool.get_total_blocks();
        double pool_pct = (total_pool_blocks > 0) ? (static_cast<double>(used_pool_blocks) / total_pool_blocks) * 100.0 : 0.0;

        // Storage Yüzdesi
        double disk_pct = (stats.disk_total_bytes > 0) ? (static_cast<double>(stats.disk_used_bytes) / stats.disk_total_bytes) * 100.0 : 0.0;

        // ANSI Terminali Temizleme ve Başa Alma
        std::cout << "\033[H\033[J";

        std::cout << "\033[1;36m╔══════════════════════════════════════════════════════════════════════════╗\033[0m\n";
        std::cout << "\033[1;36m║     ⚡ HIGH-PERFORMANCE MULTI-THREADED HTTP SERVER LIVE DASHBOARD ⚡     ║\033[0m\n";
        std::cout << "\033[1;36m╚══════════════════════════════════════════════════════════════════════════╝\033[0m\n";

        // Uptime & Genel Durum
        std::cout << " \033[1;33m● Port:\033[0m " << PORT 
                  << "  |  \033[1;33m● Uptime:\033[0m " << std::setfill('0') << std::setw(2) << uptime_h << ":" 
                  << std::setfill('0') << std::setw(2) << uptime_m << ":" 
                  << std::setfill('0') << std::setw(2) << uptime_s
                  << "  |  \033[1;33m● Aktif Bağlantı:\033[0m " << g_metrics.active_connections.load() << "\n\n";

        // 1. CPU KULLANIMI
        std::cout << "\033[1;32m[CPU & İŞ PARÇACIKLARI]\033[0m\n";
        std::cout << "  Process CPU:  " << render_progress_bar(stats.cpu_usage_pct) 
                  << " " << std::fixed << std::setprecision(1) << stats.cpu_usage_pct << " %\n";
        std::cout << "  Worker Threads: " << thread_pool.get_worker_count() << " aktif işçi çekirdeği\n\n";

        // 2. RAM (BELLEK) KULLANIMI
        double rss_mb = stats.memory_rss_bytes / (1024.0 * 1024.0);
        double sys_ram_gb = stats.system_total_ram_bytes / (1024.0 * 1024.0 * 1024.0);
        std::cout << "\033[1;35m[RAM & BELLEK YÖNETİMİ]\033[0m\n";
        std::cout << "  Process RSS:  " << std::fixed << std::setprecision(2) << rss_mb << " MB";
        if (sys_ram_gb > 0) std::cout << " (Toplam Sistem RAM: " << sys_ram_gb << " GB)";
        std::cout << "\n";
        std::cout << "  Memory Pool:  " << render_progress_bar(pool_pct) 
                  << " " << used_pool_blocks << "/" << total_pool_blocks << " blok (" 
                  << std::fixed << std::setprecision(1) << pool_pct << " %)\n";
        std::cout << "  Pool Rezerv:  " << (total_pool_blocks * mem_pool.get_block_size()) / 1024 << " KB Sabit Blok Havuzu\n\n";

        // 3. STORAGE (DİSK) KULLANIMI
        double disk_used_gb = stats.disk_used_bytes / (1024.0 * 1024.0 * 1024.0);
        double disk_total_gb = stats.disk_total_bytes / (1024.0 * 1024.0 * 1024.0);
        std::cout << "\033[1;34m[STORAGE / DİSK KULLANIMI]\033[0m\n";
        std::cout << "  Disk Alanı:   " << render_progress_bar(disk_pct) 
                  << " " << std::fixed << std::setprecision(1) << disk_used_gb << " GB / " 
                  << disk_total_gb << " GB (" << disk_pct << " %)\n\n";

        // 4. WORK-STEALING & LOAD BALANCING
        std::cout << "\033[1;33m[LOAD BALANCING & WORK-STEALING]\033[0m\n";
        std::cout << "  Yerel Görevler (Local):   " << thread_pool.get_local_tasks() << "\n";
        std::cout << "  Çalınan Görevler (Stolen): " << thread_pool.get_stolen_tasks() 
                  << " \033[90m(Kuyruk dengeleme)\033[0m\n\n";

        // 5. AĞ & HTTP VERİLERİ
        double mb_sent = g_metrics.bytes_sent.load() / (1024.0 * 1024.0);
        std::cout << "\033[1;37m[AĞ & HTTP TRAFİĞİ]\033[0m\n";
        std::cout << "  Toplam İstek (Total):     " << g_metrics.total_requests.load() << " istek\n";
        std::cout << "  Anlık Hız (Throughput):    " << std::fixed << std::setprecision(1) << rps << " req/sec (RPS)\n";
        std::cout << "  Gönderilen Veri (Sent):   " << std::fixed << std::setprecision(2) << mb_sent << " MB\n";
        std::cout << "\033[1;36m──────────────────────────────────────────────────────────────────────────\033[0m\n";
        std::cout << " \033[90mÇıkış için Ctrl+C tuşlarına basabilirsiniz.\033[0m" << std::endl;
    }
}

// =============================================================================
// 6. ANA SUNUCU DÖNGÜSÜ
// =============================================================================
int main() {
    signal(SIGPIPE, SIG_IGN);

    // 1. Custom Memory Pool (512 blok x 8KB = 4 MB pre-allocated)
    MemoryPool memory_pool(POOL_BLOCK_SIZE, POOL_BLOCK_COUNT);

    // 2. Thread Pool (İşlemci çekirdeği kadar worker)
    unsigned int hardware_threads = std::thread::hardware_concurrency();
    size_t thread_count = (hardware_threads > 0) ? hardware_threads : 8;
    WorkStealingThreadPool thread_pool(thread_count);

    // 3. Server Socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Socket hatası: " << strerror(errno) << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind hatası: " << strerror(errno) << std::endl;
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 4096) < 0) {
        std::cerr << "Listen hatası: " << strerror(errno) << std::endl;
        close(server_fd);
        return 1;
    }

    if (!set_non_blocking(server_fd)) {
        std::cerr << "Server socket non-blocking yapılamadı!\n";
        close(server_fd);
        return 1;
    }

    // 4. Reactor Event Loop
    EventReactor reactor;
    reactor.add_socket(server_fd);

    // 5. Canlı CLI Dashboard Thread'ini Başlat
    std::atomic<bool> dashboard_running{true};
    std::thread dashboard_thread(run_cli_dashboard, std::ref(memory_pool), std::ref(thread_pool), std::ref(dashboard_running));

    // 6. Olay Dinleme Döngüsü (Event Loop)
    while (true) {
        std::vector<int> ready_fds = reactor.poll_events(1000);

        for (int fd : ready_fds) {
            if (fd == server_fd) {
                while (true) {
                    struct sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);

                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        break;
                    }

                    set_non_blocking(client_fd);
                    reactor.add_socket(client_fd);
                }
            } else {
                reactor.remove_socket(fd);
                thread_pool.submit([fd, &memory_pool]() {
                    handle_client(fd, memory_pool);
                });
            }
        }
    }

    dashboard_running = false;
    if (dashboard_thread.joinable()) {
        dashboard_thread.join();
    }

    close(server_fd);
    return 0;
}
