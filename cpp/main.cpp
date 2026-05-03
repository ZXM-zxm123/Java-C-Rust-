#include <iostream>
#include <string>
#include <cstdint>
#include <cstring>
#include <thread>
#include <chrono>
#include <boost/asio.hpp>
#include <zmq.hpp>

using namespace boost::asio;

#pragma pack(push, 1)
struct MarketDataPacket {
    uint32_t magic;
    uint8_t version;
    char symbol[8];
    double price;
    uint64_t volume;
    int64_t timestamp;
};
#pragma pack(pop)

const uint32_t MAGIC = 0x53544F43;
const uint8_t VERSION = 0x01;
const int UDP_PORT = 5000;
const std::string ZMQ_ADDR = "tcp://127.0.0.1:5555";

class MarketDataReceiver {
public:
    MarketDataReceiver(io_context& io_context, zmq::socket_t& publisher)
        : socket_(io_context, ip::udp::endpoint(ip::udp::v4(), UDP_PORT)),
          publisher_(publisher) {
        do_receive();
    }

private:
    void do_receive() {
        socket_.async_receive_from(
            buffer(recv_buffer_), sender_endpoint_,
            [this](boost::system::error_code ec, std::size_t length) {
                if (!ec && length >= sizeof(MarketDataPacket)) {
                    process_packet(recv_buffer_.data(), length);
                }
                do_receive();
            });
    }

    void process_packet(const char* data, std::size_t length) {
        const MarketDataPacket* packet = reinterpret_cast<const MarketDataPacket*>(data);
        if (packet->magic != MAGIC || packet->version != VERSION) {
            std::cerr << "Invalid packet format!" << std::endl;
            return;
        }

        std::string symbol(packet->symbol, strnlen(packet->symbol, 8));
        std::string json_msg = "{"
            "\"symbol\":\"" + symbol + "\","
            "\"price\":" + std::to_string(packet->price) + ","
            "\"volume\":" + std::to_string(packet->volume) + ","
            "\"timestamp\":" + std::to_string(packet->timestamp) +
        "}";

        std::string topic = "market_data";
        zmq::message_t topic_msg(topic.size());
        memcpy(topic_msg.data(), topic.data(), topic.size());

        zmq::message_t data_msg(json_msg.size());
        memcpy(data_msg.data(), json_msg.data(), json_msg.size());

        publisher_.send(topic_msg, zmq::send_flags::sndmore);
        publisher_.send(data_msg, zmq::send_flags::none);

        std::cout << "Published: " << json_msg << std::endl;
    }

    ip::udp::socket socket_;
    ip::udp::endpoint sender_endpoint_;
    std::array<char, 1024> recv_buffer_;
    zmq::socket_t& publisher_;
};

void send_mock_data() {
    io_context io_context;
    ip::udp::socket socket(io_context, ip::udp::endpoint(ip::udp::v4(), 0));
    ip::udp::endpoint destination(ip::address::from_string("127.0.0.1"), UDP_PORT);

    std::vector<std::string> symbols = {"000001", "600000", "000002", "600036"};
    std::vector<double> prices = {10.5, 50.2, 25.8, 35.5};

    while (true) {
        for (size_t i = 0; i < symbols.size(); ++i) {
            MarketDataPacket packet;
            packet.magic = MAGIC;
            packet.version = VERSION;
            strncpy(packet.symbol, symbols[i].c_str(), 8);
            packet.price = prices[i] + (rand() % 100 - 50) * 0.01;
            packet.volume = rand() % 1000000;
            packet.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            socket.send_to(buffer(&packet, sizeof(packet)), destination);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

int main() {
    try {
        zmq::context_t context(1);
        zmq::socket_t publisher(context, ZMQ_PUB);
        publisher.bind(ZMQ_ADDR);

        std::cout << "C++ Market Data Receiver starting..." << std::endl;
        std::cout << "UDP port: " << UDP_PORT << std::endl;
        std::cout << "ZeroMQ PUB: " << ZMQ_ADDR << std::endl;

        std::thread mock_thread(send_mock_data);

        io_context io_context;
        MarketDataReceiver receiver(io_context, publisher);
        io_context.run();

        mock_thread.join();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
