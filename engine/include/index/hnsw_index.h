#ifndef VECTOR_DATABASE_HNSW_INDEX_H
#define VECTOR_DATABASE_HNSW_INDEX_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

class HNSWIndex : public VectorIndex {
public:
    enum class DistanceMetric {
        L2,
        Cosine
    };

    explicit HNSWIndex(std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, DistanceMetric metric, int vector_dimensionality);

    void Insert(Id id, const Vector& vector) override;
    void Remove(Id id) override;
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t ef_search) const override;

    void Cleanup() override;

private:
    struct Node {
        Vector vector;
        int level;
        std::unordered_map<int, std::unordered_set<Id>> neighbors;
        bool active;
    };

    std::unordered_map<Id, Node> nodes_;
    std::unordered_map<int, std::unordered_set<Id>> node_levels_;

    int vector_dimensionality_;

    int max_level_;
    std::optional<Id> entry_point_;

    std::size_t ef_construction_;
    std::size_t m_;
    std::size_t m0_;
    float ml_;
    DistanceMetric metric_;

    mutable std::mt19937 random_engine_;
    mutable std::uniform_real_distribution<> level_distribution_;

    mutable std::shared_mutex rw_mutex_;

    std::vector<Id> SearchLevel(const Vector& query, std::optional<Id> entry_point, std::size_t ef, int level) const;
    std::vector<Id> SelectNeighbors(const Vector& query, const std::vector<Id>& candidates, int level) const;

    float ComputeDistance(const Vector& a, const Vector& b) const;
    void ConnectNeighbors(Id node_id, const Vector& vector, const std::vector<Id>& neighbors, int level);
    int GetRandomLevel() const;
};

} // vector_db_engine

#endif //VECTOR_DATABASE_HNSW_INDEX_H
