#include "persistence_manager.h"

#include "vector_store.h"

namespace vector_db_engine {

VectorPersistenceManager::VectorPersistenceManager(std::string store_snapshot_file_path, std::string index_snapshot_file_path)
    : store_snapshot_file_path_(store_snapshot_file_path),
      index_snapshot_file_path_(index_snapshot_file_path)
{}

void VectorPersistenceManager::SaveSnapshot(const VectorStore& store) const {
    std::ofstream out_store(store_snapshot_file_path_);
    if (!out_store) {
        return;
    }

    std::ofstream out_index(index_snapshot_file_path_);
    if (!out_index) {
        return;
    }
}

void VectorPersistenceManager::LoadSnapshot(VectorStore& store) const {

}

}
