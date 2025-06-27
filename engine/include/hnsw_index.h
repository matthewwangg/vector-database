#ifndef VECTOR_DATABASE_HNSW_INDEX_H
#define VECTOR_DATABASE_HNSW_INDEX_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "vector_index.h"

class HNSWIndex : public VectorIndex {
public:
    explicit HNSWIndex(std::size_t m, std::size_t ef_construction, float ml);

    void Insert(Id id, const Vector& vector) override;
    void Remove(Id id) override;
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t ef_search) const override;

private:
    struct Node {
        Vector vector;
        int level;
        std::unordered_map<int, std::unordered_set<Id>> neighbors;
    };

    std::unordered_map<Id, Node> nodes_;

    int max_level_;

    std::optional<Id> entry_point_;

    std::size_t ef_construction_;
    std::size_t m_;
    float ml_;

    mutable std::mt19937 random_engine_;
    mutable std::uniform_real_distribution<> level_distribution_;

    int GetRandomLevel() const;
};

#endif //VECTOR_DATABASE_HNSW_INDEX_H
