cmake_minimum_required(VERSION 3.27)
project(vector_database)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(engine)
add_subdirectory(server)

enable_testing()

include(FetchContent)

set(gmock_build_tests OFF)
set(BUILD_GMOCK ON)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.17.0
)

FetchContent_MakeAvailable(googletest)

add_executable(vector_db_tests
    engine/tests/hnsw_index_test.cc
)

target_link_libraries(vector_db_tests
        PRIVATE
        engine
        server
        GTest::gmock_main
)

include(GoogleTest)
gtest_discover_tests(vector_db_tests)
