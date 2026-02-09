#include <pybind11/pybind11.h>
// #include "shawlynotvol/shawlynotvol.h"

int add(int a, int b) {
    return a + b;
}

PYBIND11_MODULE(_shawlynotvol, m) {
    m.doc() = "Native C++ extensions for mypackage";
    m.def("add", &add, "Add two integers", pybind11::arg("a"), pybind11::arg("b"));
}