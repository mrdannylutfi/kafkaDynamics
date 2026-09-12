cmake_minimum_required(VERSION 3.15)
project(KafkaDynamoDBSink C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Handle vcpkg / conan toolchain integrations automatically if present
if(DEFINED ENV{VCPKG_ROOT} AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
endif()

# Find dependencies across multiple platforms
find_package(RdKafka CONFIG REQUIRED)
find_package(cjson CONFIG REQUIRED)

# Build Target
add_executable(kafka_sink main.c)

# Modern target-based dependency linking (Handles include paths automatically)
target_link_libraries(kafka_sink PRIVATE 
    rdkafka::rdkafka
    cjson
)

# On Windows platforms, explicitly link system socket & helper configurations
if(WIN32)
    target_link_libraries(kafka_sink PRIVATE ws2_32 signand)
endif()
