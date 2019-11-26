cmake_minimum_required(VERSION 3.15)
project(csv_to_arrow_cpp)

set(CMAKE_CXX_STANDARD 14)


add_executable(csv_to_arrow main.cpp select.cpp select.h util/util.cpp util/util.h BuildFilter.cpp BuildFilter.h BloomFilter.cpp BloomFilter.h Join.cpp Join.h JoinExecutor.cpp JoinExecutor.h Queries.cpp Queries.h util/BloomFilterTest.cpp util/BloomFilterTest.h)

include_directories( ./third-party/apache-arrow/0.15.1/include )
link_directories( ./third-party/apache-arrow/0.15.1/lib )

file(GLOB LIBRARIES "./third-party/arrow/cpp/release/release/libarrow.so")
message("LIBRARIES = ${LIBRARIES}")

target_link_libraries(csv_to_arrow  ${LIBRARIES})
