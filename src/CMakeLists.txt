
add_library(src select.cpp select.h ../util/util.cpp ../util/util.h BuildFilter.cpp BuildFilter.h BloomFilter.cpp BloomFilter.h Join.cpp Join.h JoinExecutor.cpp JoinExecutor.h Queries.cpp Queries.h ../util/BloomFilterTest.cpp ../util/BloomFilterTest.h)

set_source_files_properties(src PRIVATE DEBUG=0 CR=0)
target_include_directories(src PUBLIC ${ARROW_INCLUDE_DIR} )
target_link_libraries(src PUBLIC ${ARROW_SHARED_LIB})