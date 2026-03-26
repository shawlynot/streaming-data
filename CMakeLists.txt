cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

find_package(Boost REQUIRED)

set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ version selection")

add_subdirectory(cpp/src)

if(SKBUILD)
    add_subdirectory(cpp/py)
else()
    enable_testing()
    add_subdirectory(cpp/test)
endif()