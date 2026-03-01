pybind11_add_module(_core MODULE
    _core.cpp
)

target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

message("BUILDDIR='${CMAKE_CURRENT_BINARY_DIR}'")

install(TARGETS _core LIBRARY DESTINATION .) # I don't understand how this works
