#ifndef VECTOR_DATABASE_PERSISTENCE_MANAGER_H
#define VECTOR_DATABASE_PERSISTENCE_MANAGER_H

#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "flat_index.h"
#include "hnsw_index.h"
#include "logger.h"
#include "vector_store.h"

#include "replica.pb.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorPersistenceManager {
public:
    enum class StoredIndexType {
        HNSW,
        FLAT,
    };

    VectorPersistenceManager(const std::string& name, StoredIndexType stored_index_type, const std::string& store_snapshot_file_path, const std::string& index_snapshot_file_path, const std::string& wal_file_path, const std::string& metadata_file_path, Logger* logger);

    void SaveSnapshot(const VectorStore& store) const;
    std::unique_ptr<VectorStore> LoadSnapshot() const;

    void SaveMetadata(int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) const;

    void AppendInsert(Id id, const Vector& vector, const std::string& content);
    void AppendRemove(Id id);
    void AppendCreate(const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);
    void AppendDrop();

    void ReplayWAL(const std::function<void(const vector_db::WALEntry&)>& callback);
    void ClearWAL();
    void Clear();

    std::vector<vector_db::WALEntry> SerializeWALEntries(int offset);

private:
    std::string GetFullFilepath(const std::string& file_path);

    std::string name_;
    std::string table_name_;

    StoredIndexType stored_index_type_;
    std::string metadata_file_path_;

    std::string store_snapshot_file_path_;
    std::string index_snapshot_file_path_;

    std::string wal_file_path_;
    std::ofstream wal_out_;
    std::mutex wal_log_mutex_;

    Logger* logger_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_PERSISTENCE_MANAGER_H
