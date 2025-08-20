#ifndef VECTOR_DATABASE_FLAT_INDEX_H
#define VECTOR_DATABASE_FLAT_INDEX_H

#include <cstdint>
#include <memory>
#include <shared_mutex>
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

    // Creates the flat index without any previous data to load.
    explicit FlatIndex(const FlatIndexConfig& config);

    // Creates the flat index and initializes it with the loaded previous data.
    explicit FlatIndex(const FlatIndexConfig& config, std::unordered_map<Id, Vector> vectors);

    // Insert the vector into the flat index.
    void Insert(Id id, const Vector& vector) override;

    // Remove the vector by ID from the flat index.
    void Remove(Id id) override;

    // Search the flat index for the k-nearest neighbors (or approximate nearest neighbors) to the query vector.
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t search_param) const override;

    // Does nothing. No need for cleanup on the flat index.
    void Cleanup() override;

    // Does nothing. No need for reindex on the flat index.
    void Reindex() override;

    FlatIndexConfig GetConfig() const { return config_; }
    std::unordered_map<Id, Vector> GetVectors() const { return vectors_; }

private:
    // Compute the distance between two vectors using the configured distance metric.
    float ComputeDistance(const Vector& a, const Vector& b) const;

    std::unordered_map<Id, Vector> vectors_;

    FlatIndexConfig config_;

    mutable std::shared_mutex rw_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_FLAT_INDEX_H
