cmake_minimum_required(VERSION 3.16)

project(SirenOptAcquisition LANGUAGES CXX)

find_package(Threads REQUIRED)

add_library(sirenopt_core STATIC
    src/types.cpp
    src/sensor_b.cpp
    src/batch_queue.cpp
    src/acquisition.cpp
)

target_include_directories(sirenopt_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(sirenopt_core PUBLIC cxx_std_17)
target_link_libraries(sirenopt_core PUBLIC Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(sirenopt_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

include(CTest)

if(BUILD_TESTING)
    add_executable(foundation_smoke tests/foundation_smoke.cpp)
    target_link_libraries(foundation_smoke PRIVATE sirenopt_core)
    add_test(NAME foundation_smoke COMMAND foundation_smoke)

    add_executable(sensor_b_tests tests/sensor_b_tests.cpp)
    target_link_libraries(sensor_b_tests PRIVATE sirenopt_core)
    add_test(NAME sensor_b_tests COMMAND sensor_b_tests)

    add_executable(acquisition_tests tests/acquisition_tests.cpp)
    target_link_libraries(acquisition_tests PRIVATE sirenopt_core)
    add_test(NAME acquisition_tests COMMAND acquisition_tests)
endif()
