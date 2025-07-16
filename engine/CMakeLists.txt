add_library(engine
        src/engine.cc
        src/store/vector_store.cc
        src/index/hnsw_index.cc
        src/storage/persistence_manager.cc
        src/util/thread_pool.cc
        src/cache/lru_cache.cc
        src/replica/replica_manager_service_impl.cc
        proto/storage.pb.cc
        proto/replica.pb.cc
        proto/replica.grpc.pb.cc
)

target_include_directories(engine PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/index
        ${CMAKE_CURRENT_SOURCE_DIR}/include/store
        ${CMAKE_CURRENT_SOURCE_DIR}/include/storage
        ${CMAKE_CURRENT_SOURCE_DIR}/include/util
        ${CMAKE_CURRENT_SOURCE_DIR}/include/cache
        ${CMAKE_CURRENT_SOURCE_DIR}/include/replica
        ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

target_link_libraries(engine PRIVATE
        grpc++
        grpc
        protobuf
)
