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
    struct Node {
        Vector vector;
        int level;
        std::unordered_map<int, std::unordered_set<Id>> neighbors;
        bool active;
    };

    enum class DistanceMetric {
        L2,
        Cosine
    };

    explicit HNSWIndex(std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, DistanceMetric metric, int vector_dimensionality);
    explicit HNSWIndex(std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, DistanceMetric metric, int vector_dimensionality, int max_level, std::optional<Id> entry_point, std::unordered_map<Id, Node> nodes, std::unordered_map<int, std::unordered_set<Id>> node_levels);

    void Insert(Id id, const Vector& vector) override;
    void Remove(Id id) override;
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t ef_search) const override;

    void Cleanup() override;
    void Reindex() override;

    const std::unordered_map<Id, Node>& GetNodes() const { return nodes_; }
    const std::unordered_map<int, std::unordered_set<Id>>& GetNodeLevels() const { return node_levels_; }

    std::size_t GetM() const { return m_; }
    std::size_t GetM0() const { return m0_; }
    std::size_t GetEfConstruction() const { return ef_construction_; }
    float GetML() const { return ml_; }
    DistanceMetric GetMetric() const { return metric_; }
    int GetVectorDimensionality() const { return vector_dimensionality_; }

    int GetMaxLevel() const { return max_level_; }
    std::optional<Id> GetEntryPoint() const { return entry_point_; }

private:
    std::vector<Id> SearchLevel(const Vector& query, std::optional<Id> entry_point, std::size_t ef, int level) const;
    std::vector<Id> SelectNeighbors(const Vector& query, const std::vector<Id>& candidates, int level) const;

    void InsertNoLock(Id id, const Vector& vector);

    float ComputeDistance(const Vector& a, const Vector& b) const;
    void ConnectNeighbors(Id node_id, const Vector& vector, const std::vector<Id>& neighbors, int level);
    int GetRandomLevel(Id id) const;

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
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_HNSW_INDEX_H
