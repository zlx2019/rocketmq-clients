cmake_minimum_required(VERSION 3.30)
project(rocketmq_cpp_client_v2)

set(CMAKE_CXX_STANDARD 11)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${Boost_INCLUDE_DIRS})

# RocketMQ 头文件路径
include_directories(/usr/local/include/rocketmq)
# RocketMQ 库路径
link_directories(lib/arm64)

# 编译所有源文件
file(GLOB files "src/*.c*")
foreach(file ${files})
    get_filename_component(basename ${file} NAME_WE)
    # 创建可执行文件
    add_executable(${basename} ${file})
    # 链接RocketMQ 库
    target_link_libraries (${basename} rocketmq)
endforeach()