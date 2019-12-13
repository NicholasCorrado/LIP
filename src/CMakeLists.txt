
add_library(src select.cpp select.h ../util/util.cpp ../util/util.h BuildFilter.cpp BuildFilter.h BloomFilter.cpp BloomFilter.h Join.cpp Join.h JoinExecutor.cpp JoinExecutor.h Queries.cpp Queries.h ../util/BloomFilterTest.cpp ../util/BloomFilterTest.h)

target_include_directories(src PUBLIC ${ARROW_INCLUDE_DIR} )
target_link_libraries(src PUBLIC ${ARROW_SHARED_LIB})