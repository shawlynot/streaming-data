cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

set(PYBIND11_FINDPYTHON ON)

find_package(pybind11 REQUIRED CONFIG)
find_package(Boost REQUIRED)


set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ version selection")

add_subdirectory(cpp)