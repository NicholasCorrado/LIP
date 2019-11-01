cmake_minimum_required(VERSION 3.15)
project(csv_to_arrow_cpp)

set(CMAKE_CXX_STANDARD 14)

add_executable(csv_to_arrow main.cpp main.h select.cpp select.h)

include_directories( /usr/local/Cellar/apache-arrow/0.15.0/include )
link_directories( /usr/local/Cellar/apache-arrow/0.15.0/lib )

file(GLOB LIBRARIES "/usr/local/Cellar/apache-arrow/0.15.0/lib/*w.dylib")
message("LIBRARIES = ${LIBRARIES}")

target_link_libraries(csv_to_arrow  ${LIBRARIES})