cmake_minimum_required(VERSION 3.17)
project(KuduExample)

set(CMAKE_CXX_STANDARD 11)

add_executable(kudu-test main.cpp)

target_link_libraries(kudu-test kudu_client hs2client pthread)