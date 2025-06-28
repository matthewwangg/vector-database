#include "hnsw_index.h"

#include <queue>
#include <random>

namespace vector_db_engine {

HNSWIndex::HNSWIndex(std::size_t m, std::size_t ef_construction, float ml)
    : m_(m),
      ef_construction_(ef_construction),
      ml_(ml),
      max_level_(-1),
      random_engine_(std::random_device{}()),
      level_distribution_(0.0, 1.0),
      metric_(DistanceMetric::L2)
{}

void HNSWIndex::Insert(Id id, const Vector& vector) {

}

void HNSWIndex::Remove(Id id) {

}

std::vector<Id> HNSWIndex::Search(const Vector& query, std::size_t k, std::size_t ef_search) const {
    return {};
}

std::vector<Id> HNSWIndex::SearchLevel(const Vector& query, std::optional<Id> entry_point, std::size_t ef, int level) const {
    if (!entry_point.has_value() || ef <= 0) {
        return {};
    }

    std::priority_queue<std::pair<float, Id>, std::vector<std::pair<float, Id>>, std::greater<>> candidates;
    std::priority_queue<std::pair<float, Id>> top_ef;
    std::unordered_set<Id> visited;

    Id entry_point_id = entry_point.value();
    float entry_point_distance = ComputeDistance(query, nodes_.at(entry_point_id).vector);

    candidates.emplace(entry_point_distance, entry_point_id);
    top_ef.emplace(entry_point_distance, entry_point_id);
    visited.insert(entry_point_id);

    while (!candidates.empty()) {
        auto [distance, node_id] = candidates.top();
        candidates.pop();
        visited.insert(node_id);

        if (top_ef.size() == ef) {
            if (distance > top_ef.top().first) {
                break;
            }
            top_ef.pop();
        }

        top_ef.emplace(std::pair<float, Id>(distance, node_id));

        for (Id neighbor : nodes_.at(node_id).neighbors.at(level)) {
            if (visited.contains(neighbor)) {
                continue;
            }
            float neighbor_distance = ComputeDistance(query, nodes_.at(neighbor).vector);
            candidates.emplace(std::pair<float, Id>(neighbor_distance, neighbor));
        }
    }

    std::vector<Id> nearest;

    while (!top_ef.empty()) {
        nearest.push_back(top_ef.top().second);
        top_ef.pop();
    }

    return nearest;
}

float HNSWIndex::ComputeDistance(const vector_db_engine::Vector& a, const vector_db_engine::Vector& b) const {
    if (a.size() != b.size()) {
        return std::numeric_limits<float>::infinity();
    }

    if (metric_ == DistanceMetric::L2) {
        float distance = 0.0f;

        for (int i = 0; i < a.size(); ++i) {
            float diff = a[i] - b[i];
            distance += diff * diff;
        }
        return std::sqrt(distance);
    }

    if (metric_ == DistanceMetric::Cosine) {
        float dot_product = 0.0f;
        float a_norm = 0.0f;
        float b_norm = 0.0f;

        for (int i = 0; i < a.size(); ++i) {
            dot_product += a[i] * b[i];
            a_norm += a[i] * a[i];
            b_norm += b[i] * b[i];
        }

        float denominator = std::sqrt(a_norm) * std::sqrt(b_norm);
        if (denominator == 0) {
            return 1.0f;
        }

        return 1.0f - dot_product / denominator;
    }

    return std::numeric_limits<float>::infinity();
}

int HNSWIndex::GetRandomLevel() const {
    return static_cast<int>(-std::log(1.0 - level_distribution_(random_engine_)) * ml_);
}

} // namespace vector_db_engine
