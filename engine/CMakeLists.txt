add_library(engine
        src/engine.cc
        src/store/vector_store.cc
        src/index/hnsw_index.cc
)

target_include_directories(engine PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/index
        ${CMAKE_CURRENT_SOURCE_DIR}/include/store
)
