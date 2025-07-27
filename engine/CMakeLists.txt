add_library(engine
        src/engine.cc
        src/store/vector_store.cc
        src/index/hnsw_index.cc
        src/storage/persistence_manager.cc
        src/util/thread_pool.cc
        src/cache/lru_cache.cc
        src/replica/replica_manager_service_impl.cc
        src/logger/local_logger.cc
        src/logger/remote_logger.cc
        src/metrics/metrics_manager.cc
        src/stats/stats_manager.cc
        src/replica/replica_manager.cc
        src/cleaner/cleaner.cc
        proto/storage.pb.cc
        proto/replica.pb.cc
        proto/replica.grpc.pb.cc
        proto/log.grpc.pb.cc
        proto/log.pb.cc
)

target_include_directories(engine PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/index
        ${CMAKE_CURRENT_SOURCE_DIR}/include/store
        ${CMAKE_CURRENT_SOURCE_DIR}/include/storage
        ${CMAKE_CURRENT_SOURCE_DIR}/include/util
        ${CMAKE_CURRENT_SOURCE_DIR}/include/cache
        ${CMAKE_CURRENT_SOURCE_DIR}/include/replica
        ${CMAKE_CURRENT_SOURCE_DIR}/include/logger
        ${CMAKE_CURRENT_SOURCE_DIR}/include/metrics
        ${CMAKE_CURRENT_SOURCE_DIR}/include/stats
        ${CMAKE_CURRENT_SOURCE_DIR}/include/cleaner
        ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

target_link_libraries(engine PRIVATE
        grpc++
        grpc
        protobuf
        gpr
        absl_synchronization
        absl_time
        absl_strings
)
