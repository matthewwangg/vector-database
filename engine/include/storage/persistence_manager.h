#ifndef VECTOR_DATABASE_STORAGE_H
#define VECTOR_DATABASE_STORAGE_H

#include <string>

#include "vector_store.h"

namespace vector_db_engine {

class VectorPersistenceManager {
public:
    VectorPersistenceManager(std::string store_snapshot_file_path, std::string index_snapshot_file_path);

    void SaveSnapshot(const VectorStore& store) const;
    void LoadSnapshot(VectorStore& store) const;

private:
    std::string store_snapshot_file_path_;
    std::string index_snapshot_file_path_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_STORAGE_H
