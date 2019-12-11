project(csv_to_arrow_cpp)

set(CMAKE_CXX_STANDARD 14)

add_executable(csv_to_arrow main.cpp select.cpp select.h util/util.cpp util/util.h BuildFilter.cpp BuildFilter.h BloomFilter.cpp BloomFilter.h Join.cpp Join.h JoinExecutor.cpp JoinExecutor.h Queries.cpp Queries.h util/BloomFilterTest.cpp util/BloomFilterTest.h)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(Arrow REQUIRED)

include_directories( ${ARROW_INCLUDE_DIR} )
link_directories( ${ARROW_SHARED_LIB} )

target_link_libraries(csv_to_arrow  ${ARROW_SHARED_LIB})
