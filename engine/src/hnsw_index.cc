#include "hnsw_index.h"

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
    return {};
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
