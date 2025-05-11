// abx_client.cpp

#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <fstream>

using json = nlohmann::json;

struct Packet {
    std::string symbol;
    char buysellindicator;
    int32_t quantity;
    int32_t price;
    int32_t packetSequence;
};

constexpr const char* SERVER_IP = "127.0.0.1";
constexpr uint16_t SERVER_PORT = 3000;
constexpr size_t PACKET_SIZE = 17;
constexpr int TOTAL_SEQUENCES = 14;

int connect_to_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

    return sock;
}

void send_stream_all(int sock) {
    uint8_t payload[2] = {1, 0};
    send(sock, payload, 2, 0);
}

void send_resend_packet(int sock, uint8_t sequence) {
    uint8_t payload[2] = {2, sequence};
    send(sock, payload, 2, 0);
}

Packet parse_packet(const uint8_t* buffer) {
    Packet pkt;
    pkt.symbol = std::string(reinterpret_cast<const char*>(buffer), 4);
    pkt.buysellindicator = buffer[4];
    pkt.quantity = ntohl(*reinterpret_cast<const int32_t*>(buffer + 5));
    pkt.price = ntohl(*reinterpret_cast<const int32_t*>(buffer + 9));
    pkt.packetSequence = ntohl(*reinterpret_cast<const int32_t*>(buffer + 13));
    return pkt;
}

void save_to_json(const std::map<int, Packet>& packets) {
    json output = json::array();
    for (const auto& [seq, pkt] : packets) {
        output.push_back({
            {"sequence", pkt.packetSequence},
            {"symbol", pkt.symbol},
            {"buysellindicator", std::string(1, pkt.buysellindicator)},
            {"quantity", pkt.quantity},
            {"price", pkt.price}
        });
    }
    std::ofstream file("output.json");
    file << output.dump(4);
    std::cout << "Saved to output.json\n";
}

int main() {
    std::map<int, Packet> received_packets;
    std::set<int> received_sequences;

    int sock = connect_to_server();
    send_stream_all(sock);

    uint8_t buffer[PACKET_SIZE];
    while (true) {
        ssize_t n = recv(sock, buffer, PACKET_SIZE, MSG_WAITALL);
        if (n <= 0) break;
        Packet pkt = parse_packet(buffer);
        received_packets[pkt.packetSequence] = pkt;
        received_sequences.insert(pkt.packetSequence);
    }
    close(sock);

    // Find missing sequences
    std::set<int> missing;
    for (int seq = 1; seq <= TOTAL_SEQUENCES; ++seq) {
        if (received_sequences.find(seq) == received_sequences.end()) {
            missing.insert(seq);
        }
    }

    // Resend for missing
    for (int seq : missing) {
        int sock2 = connect_to_server();
        send_resend_packet(sock2, seq);
        ssize_t n = recv(sock2, buffer, PACKET_SIZE, MSG_WAITALL);
        if (n == PACKET_SIZE) {
            Packet pkt = parse_packet(buffer);
            received_packets[pkt.packetSequence] = pkt;
        }
        close(sock2);
    }

    save_to_json(received_packets);
    return 0;
}
