add_library(core_impl core_impl.cpp)
set_target_properties(core_impl PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(core_impl PUBLIC ${CMAKE_SOURCE_DIR}/cpp/include)
target_link_libraries(core_impl PUBLIC Boost::boost)
