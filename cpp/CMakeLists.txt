cmake_minimum_required(VERSION 3.14)
project(MarketDataReceiver)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost 1.70 REQUIRED COMPONENTS system)
find_package(cppzmq REQUIRED)

add_executable(market_data_receiver main.cpp)
target_link_libraries(market_data_receiver PRIVATE
    Boost::system
    cppzmq
)
