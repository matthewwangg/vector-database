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

// VectorPersistenceManager is responsible for data persistence including snapshots, write-ahead log, and metadata files. It handles both saving and loading those files (excluding loading metadata).
class VectorPersistenceManager {
public:
    enum class StoredIndexType {
        HNSW,
        FLAT,
    };

    // Creates the vector persistence manager and creates the WAL file.
    VectorPersistenceManager(const std::string& name, StoredIndexType stored_index_type, const std::string& store_snapshot_file_path, const std::string& index_snapshot_file_path, const std::string& wal_file_path, const std::string& metadata_file_path, Logger* logger);

    // Persist the vector store and vector index directly to their respective snapshot files. Each are stored in separate files to allow for separate loading in future.
    void SaveSnapshot(const VectorStore& store) const;

    // Load the vector store and vector index from the snapshot files to memory.
    std::unique_ptr<VectorStore> LoadSnapshot() const;

    // Persist the table metadata directly to the metadata file.
    void SaveMetadata(int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) const;

    // Append an insert operation to the WAL file.
    void AppendInsert(Id id, const Vector& vector, const std::string& content);

    // Append a remove operation to the WAL file.
    void AppendRemove(Id id);

    // Append a create operation to the WAL file.
    void AppendCreate(const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);

    // Append a drop operation to the WAL file.
    void AppendDrop();

    // Replay the WAL on startup after a crash.
    void ReplayWAL(const std::function<void(const vector_db::WALEntry&)>& callback);

    // Truncate the WAL file.
    void ClearWAL();

    // Clear out all the snapshot, metadata, and WAL files on a table drop operation.
    void Clear();

    // Convert the WAL file into a vector of WAL entries starting at a specified file offset.
    std::vector<vector_db::WALEntry> SerializeWALEntries(std::uint64_t offset);

private:
    // Return the full filepath that includes the base path and the HOME directory and /.vector_db/ together.
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
