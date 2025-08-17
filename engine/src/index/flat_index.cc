#include "flat_index.h"
#include <iostream>

#include <algorithm>
#include <cmath>
#include <limits>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace vector_db_engine {

FlatIndex::FlatIndex(const FlatIndexConfig& config)
    : config_(config)
{}

FlatIndex::FlatIndex(const FlatIndexConfig& config, std::unordered_map<Id, Vector> vectors)
    : config_(config),
      vectors_(vectors)
{}

void FlatIndex::Insert(Id id, const Vector& vector) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (vector.size() != config_.vector_dimensionality || vectors_.contains(id)) {
        return;
    }
    vectors_.insert({id, vector});
}

void FlatIndex::Remove(Id id) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    vectors_.erase(id);
}

std::vector<Id> FlatIndex::Search(const Vector& query, std::size_t k, std::size_t search_param) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    if (query.size() != config_.vector_dimensionality) {
        return {};
    }

    std::priority_queue<std::pair<float, Id>> top_k;
    for (const auto& [id, vector] : vectors_) {
        float distance = ComputeDistance(query, vector);
        top_k.emplace(distance, id);

        if (top_k.size() > k) {
            top_k.pop();
        }
    }

    std::vector<Id> nearest;
    while (!top_k.empty()) {
        nearest.push_back(top_k.top().second);
        top_k.pop();
    }
    std::reverse(nearest.begin(), nearest.end());

    return nearest;
}

void FlatIndex::Cleanup() {
    return;
}

void FlatIndex::Reindex() {
    return;
}

float FlatIndex::ComputeDistance(const Vector& a, const Vector& b) const {
    if (a.size() != b.size()) {
        return std::numeric_limits<float>::infinity();
    }

    if (config_.metric == DistanceMetric::L2) {
        float distance = 0.0f;

        for (size_t i = 0; i < a.size(); ++i) {
            float diff = a[i] - b[i];
            distance += diff * diff;
        }
        return std::sqrt(distance);
    }

    if (config_.metric == DistanceMetric::Cosine) {
        float dot_product = 0.0f;
        float a_norm = 0.0f;
        float b_norm = 0.0f;

        for (size_t i = 0; i < a.size(); ++i) {
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

} // namespace vector_db_engine
