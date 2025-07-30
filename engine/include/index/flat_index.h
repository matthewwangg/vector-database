#ifndef VECTOR_DATABASE_FLAT_INDEX_H
#define VECTOR_DATABASE_FLAT_INDEX_H

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

class FlatIndex : public VectorIndex {
public:
    struct FlatIndexConfig {
        int vector_dimensionality;
        DistanceMetric metric;
    };

    explicit FlatIndex(const FlatIndexConfig& config);

    void Insert(Id id, const Vector& vector) override;
    void Remove(Id id) override;
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t search_param) const override;

    void Cleanup() override;
    void Reindex() override;

    FlatIndexConfig GetConfig() const { return config_; }

private:
    float ComputeDistance(const Vector& a, const Vector& b) const;

    std::unordered_map<Id, Vector> vectors_;

    FlatIndexConfig config_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_FLAT_INDEX_H
