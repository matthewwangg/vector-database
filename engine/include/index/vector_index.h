#ifndef VECTOR_DATABASE_VECTOR_INDEX_H
#define VECTOR_DATABASE_VECTOR_INDEX_H

#include <cstdint>
#include <vector>

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorIndex {
public:
    enum class DistanceMetric {
        L2,
        Cosine
    };

    virtual ~VectorIndex() = default;

    virtual void Insert(Id id, const Vector& vector) = 0;
    virtual void Remove(Id id) = 0;
    virtual std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t search_param) const = 0;

    virtual void Cleanup() = 0;
    virtual void Reindex() = 0;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_VECTOR_INDEX_H
