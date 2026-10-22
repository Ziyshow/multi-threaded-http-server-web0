#include <iostream>
#include <string>
#include <cstring>      // memset, strerror için
#include <sys/socket.h> // socket, bind, listen, accept fonksiyonları ve sabitleri için
#include <netinet/in.h> // sockaddr_in yapısı ve htons/htonl için
#include <unistd.h>     // close, read, write fonksiyonları için

// Sunucunun dinleyeceği port numarası
constexpr int PORT = 8080;
// Gelen istekleri okumak için tampon (buffer) boyutu
constexpr int BUFFER_SIZE = 4096;

int main() {
    // =========================================================================
    // 1. ADIM: Socket Oluşturma (socket())
    // =========================================================================
    // File Descriptor: server_fd
    // AF_INET: IPv4 protokol ailesi
    // SOCK_STREAM: TCP bağlantı tipi (güvenilir, iki yönlü akış)
    // 0: Belirtilen aile ve tip için varsayılan protokol (TCP)
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Socket oluşturulamadı! Hata: " << strerror(errno) << std::endl;
        return 1;
    }
    std::cout << "[+] Socket başarıyla oluşturuldu." << std::endl;

    // Socket seçeneklerini ayarlama (Opsiyonel ama faydalı):
    // SO_REUSEADDR: Sunucuyu kapatıp hemen tekrar açtığımızda "Address already in use"
    // hatasını engeller, portun hızlıca tekrar kullanılabilmesini sağlar.
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "setsockopt başarısız oldu! Hata: " << strerror(errno) << std::endl;
        close(server_fd);
        return 1;
    }

    // =========================================================================
    // 2. ADIM: Adres Yapılandırması ve Bağlama (bind())
    // =========================================================================
    // sockaddr_in yapısı IPv4 adres ve port bilgilerini tutar.
    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr)); // Belleği sıfırla

    server_addr.sin_family = AF_INET; // IPv4
    // INADDR_ANY: Makinedeki tüm ağ arayüzlerini (0.0.0.0) dinle
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    // htons: Host byte order'dan Network byte order'a (Big-Endian) çevirir
    server_addr.sin_port = htons(PORT);

    // Socket'i belirlediğimiz IP adresi ve porta bağlıyoruz.
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind (bağlama) başarısız! Port: " << PORT << ", Hata: " << strerror(errno) << std::endl;
        close(server_fd);
        return 1;
    }
    std::cout << "[+] Socket " << PORT << " portuna bağlandı (bind)." << std::endl;

    // =========================================================================
    // 3. ADIM: Dinlemeye Başlama (listen())
    // =========================================================================
    // backlog (burada 10): Kuyrukta bekleyebilecek maksimum bağlantı sayısı.
    // listen() çağrısından sonra socket pasif moda geçer ve gelen bağlantıları bekler.
    if (listen(server_fd, 10) < 0) {
        std::cerr << "Listen başarısız! Hata: " << strerror(errno) << std::endl;
        close(server_fd);
        return 1;
    }
    std::cout << "[+] Sunucu dinlemede: http://localhost:" << PORT << std::endl;
    std::cout << "[*] Bir tarayıcıdan veya 'curl http://localhost:" << PORT << "' ile istek bekleniyor..." << std::endl;

    // =========================================================================
    // 4. ADIM: Gelen Bağlantıyı Kabul Etme (accept())
    // =========================================================================
    // accept() gelen bağlantı olana kadar programı BLOKE EDER (bekletir).
    // Bağlantı kurulduğunda, o istemciyle iletişim kurabilmemiz için YENİ bir
    // socket dosya tanımlayıcısı (client_fd) döner.
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd < 0) {
        std::cerr << "Bağlantı kabul edilemedi (accept)! Hata: " << strerror(errno) << std::endl;
        close(server_fd);
        return 1;
    }
    std::cout << "[+] Yeni bir istemci bağlandı!" << std::endl;

    // =========================================================================
    // 5. ADIM: İstemciden Gelen HTTP İsteğini Okuma (read())
    // =========================================================================
    char buffer[BUFFER_SIZE] = {0};
    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read < 0) {
        std::cerr << "İstek okunamadı! Hata: " << strerror(errno) << std::endl;
    } else {
        std::cout << "--- Gelen HTTP İsteği (" << bytes_read << " byte) ---\n"
                  << buffer
                  << "-------------------------------------------" << std::endl;
    }

    // =========================================================================
    // 6. ADIM: HTTP Yanıtını (Response) Hazırlama ve Gönderme (write())
    // =========================================================================
    // HTTP/1.1 yanıt formatı:
    // [Protokol] [Durum Kodu] [Durum Mesajı]\r\n
    // Header1: Değer\r\n
    // Header2: Değer\r\n
    // \r\n (Header'lar ile gövde arasındaki boş satır)
    // [Gövde (Body)]
    std::string html_body = 
        "<!DOCTYPE html>"
        "<html>"
        "<head><meta charset='utf-8'><title>Web 0</title></head>"
        "<body>"
        "<h1>Kod sorunsuz çalışıyor.</h1>"
        "<p>Aşama 1 tamam.</p>"
        "</body>"
        "</html>";

    std::string http_response = 
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html; charset=UTF-8\r\n"
        "Content-Length: " + std::to_string(html_body.length()) + "\r\n"
        "Connection: close\r\n"
        "\r\n" +
        html_body;

    // Hazırlanan yanıtı istemciye soket üzerinden gönderiyoruz
    ssize_t bytes_sent = write(client_fd, http_response.c_str(), http_response.length());
    if (bytes_sent < 0) {
        std::cerr << "Yanıt gönderilemedi! Hata: " << strerror(errno) << std::endl;
    } else {
        std::cout << "[+] HTTP yanıtı başarıyla gönderildi (" << bytes_sent << " byte)." << std::endl;
    }

    // =========================================================================
    // 7. ADIM: Temizlik ve Soketleri Kapatma (close())
    // =========================================================================
    // İstemciyle işimiz bitti, istemci soketini kapatıyoruz
    close(client_fd);
    std::cout << "[+] İstemci bağlantısı kapatıldı." << std::endl;

    // Tek bir istek işleyeceğimiz için dinleyen ana soketi de kapatıyoruz
    close(server_fd);
    std::cout << "[+] Sunucu kapatıldı." << std::endl;

    return 0;
}
