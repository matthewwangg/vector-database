#ifndef VECTOR_DATABASE_VECTOR_STORE_H
#define VECTOR_DATABASE_VECTOR_STORE_H

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

// VectorStore is responsible for storage of the id, vector data, and content in memory, thread-safe write operations, ownership of the vector index, and checksum computation of the data currently stored in the vector database.
class VectorStore {
public:
    struct Data {
        Id id;
        Vector vector;
        std::string content;
    };

    enum class IndexType {
        HNSW,
        FLAT,
    };

    // Creates the vector store without any previous data to load.
    explicit VectorStore(IndexType index_type, std::unique_ptr<VectorIndex> index, int vector_dimensionality);

    // Creates the vector store and loads the previous data into the store directly.
    explicit VectorStore(IndexType index_type, std::unique_ptr<VectorIndex> index, int vector_dimensionality, std::unordered_map<Id, Data> data);

    // Insert the data into the vector store. Returns true on success and false on failure.
    bool Insert(Id id, const Vector& vector, const std::string& content);

    // Remove the data by ID from the vector store. Returns true on success and false on failure.
    bool Remove(Id id);

    // Search for the nearest data to the query, using the index. Returns the relevant data on success and returns an empty vector on failure.
    std::vector<Data> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

    // Trigger the cleanup of the soft removed index nodes for HNSW index.
    void Cleanup(bool reindex);

    // Compute the deterministic checksum of the data for replica sync validation using the IDs and content.
    std::uint64_t ComputeChecksum();

    const std::unordered_map<Id, Data>& GetStore() const { return store_; }
    const VectorIndex* GetIndex() const { return index_.get(); }
    IndexType GetIndexType() const { return index_type_; }
    int GetVectorDimensionality() const { return vector_dimensionality_; }

private:
    IndexType index_type_;

    std::unordered_map<Id, Data> store_;
    std::unique_ptr<VectorIndex> index_;

    int vector_dimensionality_;

    mutable std::shared_mutex rw_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_VECTOR_STORE_H
