#ifndef VECTOR_DATABASE_VECTOR_STORE_H
#define VECTOR_DATABASE_VECTOR_STORE_H

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorStore {
public:
    VectorStore(VectorIndex index);

    void Insert(Id id, const Vector& vector);

    void Remove(Id id);

    std::vector<Id> Search(const Vector& query, std::size_t k) const;

private:
    std::unordered_map<Id, Vector> store_;
    std::unique_ptr<VectorIndex> index_;

    int vector_dimensionality_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_VECTOR_STORE_H
