cmake_minimum_required(VERSION 3.16)
project(io_path_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(file_generator src/file_generator.cpp)
target_include_directories(file_generator PRIVATE include)

add_executable(io_path_bench src/io_path_bench.cpp)
target_include_directories(io_path_bench PRIVATE include)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(file_generator PRIVATE -Wall -Wextra -Wpedantic)
  target_compile_options(io_path_bench PRIVATE -Wall -Wextra -Wpedantic)
endif()
