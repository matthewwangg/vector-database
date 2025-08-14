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
include(GoogleTest)

add_executable(vector_db_unit_tests
        engine/tests/hnsw_index_test.cc
        engine/tests/flat_index_test.cc
        engine/tests/vector_store_test.cc
        engine/tests/persistence_manager_test.cc
        engine/tests/stats_manager_test.cc
        engine/tests/metrics_manager_test.cc
        engine/tests/cleaner_test.cc
        engine/tests/lru_cache_test.cc
        engine/tests/engine_test.cc
)

target_link_libraries(vector_db_unit_tests
        PRIVATE
        server
        GTest::gmock_main
)

gtest_discover_tests(vector_db_unit_tests)

add_executable(vector_db_e2e_tests
        server/tests/vector_database_test.cc
)

target_link_libraries(vector_db_e2e_tests
        PRIVATE
        server
        GTest::gmock_main
)

gtest_discover_tests(vector_db_e2e_tests)
