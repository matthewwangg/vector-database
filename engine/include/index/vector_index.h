#ifndef VECTOR_DATABASE_VECTOR_INDEX_H
#define VECTOR_DATABASE_VECTOR_INDEX_H

#include <cstdint>
#include <memory>
#include <vector>

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

// VectorIndex is the interface for all vector index types.
class VectorIndex {
public:
    enum class DistanceMetric {
        L2,
        Cosine
    };

    virtual ~VectorIndex() = default;

    // Insert the vector directly into the index.
    virtual void Insert(Id id, const Vector& vector) = 0;

    // Remove the vector by ID from the index.
    virtual void Remove(Id id) = 0;

    // Search the index for the k-nearest neighbors (or approximate nearest neighbors) to the query vector.
    virtual std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t search_param) const = 0;

    // Clean up the vector index of soft removed nodes, used for HNSW index.
    virtual void Cleanup() = 0;

    // Recalculate the index graph, used for HNSW index.
    virtual void Reindex() = 0;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_VECTOR_INDEX_H
