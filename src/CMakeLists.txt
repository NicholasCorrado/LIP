
add_executable(src select.cpp select.h main.cpp ../util/util.cpp ../util/util.h BuildFilter.cpp BuildFilter.h BloomFilter.cpp BloomFilter.h Join.cpp Join.h JoinExecutor.cpp JoinExecutor.h Queries.cpp Queries.h ../util/BloomFilterTest.cpp ../util/BloomFilterTest.h)

include_directories(src ${ARROW_INCLUDE_DIR} )
link_directories(src ${ARROW_SHARED_LIB} )

target_link_libraries(src  ${ARROW_SHARED_LIB})