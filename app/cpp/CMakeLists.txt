pybind11_add_module(_shawlynotvol MODULE
    shawlynotvol.cpp
)

target_include_directories(_shawlynotvol PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

message(BUILDDIR="${CMAKE_CURRENT_BINARY_DIR}")

install(TARGETS _shawlynotvol LIBRARY DESTINATION .) # I don't understand how this works
