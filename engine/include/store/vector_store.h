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

class VectorStore {
public:
    struct Data {
        Id id;
        Vector vector;
        std::string content;
    };

    enum class IndexType {
        HNSW = 0,
        FLAT = 1,
    };

    explicit VectorStore(IndexType index_type, std::unique_ptr<VectorIndex> index, int vector_dimensionality);
    explicit VectorStore(IndexType index_type, std::unique_ptr<VectorIndex> index, int vector_dimensionality, std::unordered_map<Id, Data> data);

    bool Insert(Id id, const Vector& vector, const std::string& content);
    bool Remove(Id id);
    std::vector<Data> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

    void Cleanup(bool reindex);

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
