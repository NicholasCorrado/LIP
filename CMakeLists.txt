cmake_minimum_required(VERSION 3.0)
project(db)

set(CMAKE_CXX_STANDARD 14)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(Arrow REQUIRED)

add_subdirectory(src)
add_subdirectory(apps)

