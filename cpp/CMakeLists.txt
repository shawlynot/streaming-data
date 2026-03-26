pybind11_add_module(_core MODULE
    _core.cpp
)

target_link_libraries(_core PRIVATE core_impl)

message("BUILDDIR='${CMAKE_CURRENT_BINARY_DIR}'")

install(TARGETS _core LIBRARY DESTINATION .) # I don't understand how this works
