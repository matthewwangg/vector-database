#ifndef VECTOR_DATABASE_FLAT_INDEX_H
#define VECTOR_DATABASE_FLAT_INDEX_H

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

class FlatIndex : public VectorIndex {
public:
    enum class DistanceMetric {
        L2,
        Cosine
    };

    explicit FlatIndex(DistanceMetric metric);

    void Insert(Id id, const Vector& vector) override;
    void Remove(Id id) override;
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t search_param) const override;

    void Cleanup() override;
    void Reindex() override;

private:
    float ComputeDistance(const Vector& a, const Vector& b) const;

    std::unordered_map<Id, Vector> vectors_;

    DistanceMetric metric_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_FLAT_INDEX_H
