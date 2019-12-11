add_executable(
        src
        BloomFilter.cpp
        BloomFilter.h
        BuildFilter.cpp
        BuildFilter.h
        Join.cpp
        Join.h
        JoinExecutor.cpp
        JoinExecutor.h
        main.cpp
        Queries.cpp
        Queries.h
)

target_link_libraries(src ${ARROW_SHARED_LB})
target_include_directories(src PUBLIC ${ARROW_INCLUDE_DIR})