#ifndef VECTOR_DATABASE_STORAGE_H
#define VECTOR_DATABASE_STORAGE_H

#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "vector_store.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorPersistenceManager {
public:
    VectorPersistenceManager(std::string store_snapshot_file_path, std::string index_snapshot_file_path);

    void SaveSnapshot(const VectorStore& store) const;
    std::unique_ptr<VectorStore> LoadSnapshot() const;

    void AppendInsert(Id id, const Vector& vector) const;
    void AppendRemove(Id id) const;
    void ReplayWAL(VectorStore& store) const;
    void ClearWAL() const;

private:
    std::string store_snapshot_file_path_;
    std::string index_snapshot_file_path_;

    std::string wal_file_path_;
    std::ofstream wal_out_;
    std::mutex wal_log_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_STORAGE_H
