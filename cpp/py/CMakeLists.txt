set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG)

pybind11_add_module(_core MODULE
    _core.cpp
)

target_include_directories(_core PUBLIC ${CMAKE_SOURCE_DIR}/cpp/include)
target_link_libraries(_core PRIVATE core_impl)

message("BUILDDIR='${CMAKE_CURRENT_BINARY_DIR}'")

install(TARGETS _core LIBRARY DESTINATION .)
