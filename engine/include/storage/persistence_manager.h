#ifndef VECTOR_DATABASE_STORAGE_H
#define VECTOR_DATABASE_STORAGE_H

#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "hnsw_index.h"
#include "logger.h"
#include "vector_store.h"

#include "replica.pb.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorPersistenceManager {
public:
    VectorPersistenceManager(std::string name, std::string store_snapshot_file_path, std::string index_snapshot_file_path, std::string wal_file_path, Logger* logger);

    void SaveSnapshot(const VectorStore& store) const;
    std::unique_ptr<VectorStore> LoadSnapshot() const;

    void AppendInsert(Id id, const Vector& vector, const std::string& content);
    void AppendRemove(Id id);
    void AppendCreate(int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::VectorIndex::DistanceMetric distance_metric, std::size_t cache_size);
    void AppendDrop();

    void ReplayWAL(const std::function<void(const vector_db::WALEntry&)>& callback);
    void ClearWAL();
    void Clear();

    std::vector<vector_db::WALEntry> SerializeWALEntries(int offset);

private:
    std::string GetFullFilepath(std::string file_path);

    std::string name_;
    std::string table_name_;

    std::string store_snapshot_file_path_;
    std::string index_snapshot_file_path_;

    std::string wal_file_path_;
    std::ofstream wal_out_;
    std::mutex wal_log_mutex_;

    Logger* logger_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_STORAGE_H
