add_library(engine
        src/vector_store.cc
        src/hnsw_index.cc
)

target_include_directories(engine PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
