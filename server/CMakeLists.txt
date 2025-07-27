add_executable(vector_db_server
        src/main.cc
)

target_link_libraries(vector_db_server
        server
)

target_include_directories(vector_db_server PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(server
        src/vector_db_service_impl.cc
        proto/server.grpc.pb.cc
        proto/server.pb.cc
)

target_include_directories(server PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

target_link_libraries(server PUBLIC
        engine
)
