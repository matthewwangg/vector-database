#ifndef VECTOR_DATABASE_VECTOR_INDEX_H
#define VECTOR_DATABASE_VECTOR_INDEX_H

#include <cstdint>
#include <vector>

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorIndex {
public:
    virtual ~VectorIndex() = default;

    virtual void Insert(Id id, const Vector& vector) = 0;
    virtual void Remove(Id id) = 0;
    virtual std::vector<Id> Search(const Vector& query, std::size_t k) const = 0;
};

#endif //VECTOR_DATABASE_VECTOR_INDEX_H
