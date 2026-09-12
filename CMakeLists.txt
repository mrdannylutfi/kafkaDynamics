cmake_minimum_required(VERSION 3.10)
project(KafkaDynamoDBSink C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Find System Dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka)

# Find cJSON (Supports both modern CMake config and fallback manually)
find_package(cjson CONFIG)
if(NOT cjson_FOUND)
    find_path(CJSON_INCLUDE_DIR NAMES cjson/cJSON.h)
    find_library(CJSON_LIBRARY NAMES cjson)
endif()

# Application Executable Target
add_executable(kafka_sink main.c)

# Include directories
target_include_directories(kafka_sink PRIVATE ${RDKAFKA_INCLUDE_DIRS})
if(cjson_FOUND)
    target_link_libraries(kafka_sink PRIVATE cjson)
else()
    target_include_directories(kafka_sink PRIVATE ${CJSON_INCLUDE_DIR})
    target_link_libraries(kafka_sink PRIVATE ${CJSON_LIBRARY})
endif()

# Link Libraries
target_link_libraries(kafka_sink PRIVATE ${RDKAFKA_LIBRARIES})
