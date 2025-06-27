#ifndef VECTOR_DATABASE_VECTOR_STORE_H
#define VECTOR_DATABASE_VECTOR_STORE_H

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "hnsw_index.h"

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorStore {
public:
    VectorStore(HNSWIndex index);

    void Insert(Id id, const Vector& vector);
    void Remove(Id id);
    std::vector<Id> Search(const Vector& query, std::size_t k) const;

private:
    std::unordered_map<Id, Vector> store_;
    std::unique_ptr<HNSWIndex> index_;
};

#endif //VECTOR_DATABASE_VECTOR_STORE_H
