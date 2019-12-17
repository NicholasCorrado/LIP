add_executable(main main.cpp)

target_include_directories(main PUBLIC src)
target_link_libraries(main PUBLIC src)