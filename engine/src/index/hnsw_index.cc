#include "hnsw_index.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace vector_db_engine {

HNSWIndex::HNSWIndex(std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, DistanceMetric metric, int vector_dimensionality)
    : m_(m),
      m0_(m0),
      ef_construction_(ef_construction),
      ml_(ml),
      max_level_(-1),
      random_engine_(std::random_device{}()),
      level_distribution_(0.0, 1.0),
      metric_(metric),
      vector_dimensionality_(vector_dimensionality)
{}

HNSWIndex::HNSWIndex(std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, DistanceMetric metric, int vector_dimensionality, int max_level, std::optional<Id> entry_point, std::unordered_map<Id, Node> nodes, std::unordered_map<int, std::unordered_set<Id>> node_levels)
    : m_(m),
      m0_(m0),
      ef_construction_(ef_construction),
      ml_(ml),
      max_level_(max_level),
      random_engine_(std::random_device{}()),
      level_distribution_(0.0, 1.0),
      metric_(metric),
      vector_dimensionality_(vector_dimensionality),
      entry_point_(entry_point),
      nodes_(nodes),
      node_levels_(node_levels)
{
    if (!entry_point_.has_value()) {
        for (const auto& [id, node] : nodes_) {
            if (node.level == max_level_ && node.active) {
                entry_point_ = id;
                break;
            }
        }
    }
}

void HNSWIndex::Insert(Id id, const Vector& vector) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (vector.size() != vector_dimensionality_ || nodes_.contains(id)) {
        return;
    }

    int new_level = GetRandomLevel();

    nodes_[id] = Node{
            .vector = vector,
            .level = new_level,
            .neighbors = {},
            .active = true
    };
    node_levels_[new_level].insert(id);

    if (!entry_point_.has_value()) {
        entry_point_ = id;
        max_level_ = new_level;
        return;
    }

    Id entry_point = entry_point_.value();

    for (int l = max_level_; l > new_level; --l) {
        std::vector<Id> nearest = SearchLevel(vector, entry_point, 1, l);
        if (nearest.empty()) {
            continue;
        }
        entry_point = nearest[0];
    }

    for (int l = std::min(max_level_, new_level); l > -1; --l) {
        std::vector<Id> nearest = SearchLevel(vector, entry_point, ef_construction_, l);
        std::vector<Id> neighbors = SelectNeighbors(vector, nearest, l);

        ConnectNeighbors(id, vector, neighbors, l);
    }

    if (new_level > max_level_) {
        max_level_ = new_level;
        entry_point_ = id;
    }
}

void HNSWIndex::Remove(Id id) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (!entry_point_.has_value() || !nodes_.contains(id) || !nodes_.at(id).active) {
        return;
    }

    nodes_.at(id).active = false;

    int level = nodes_.at(id).level;
    node_levels_[level].erase(id);
    if (node_levels_[level].empty()) {
        node_levels_.erase(level);
    }

    if (id != entry_point_.value()) {
        return;
    }

    entry_point_ = std::nullopt;
    int new_highest_level = max_level_;

    while (!entry_point_.has_value() && new_highest_level >= 0) {
        if (node_levels_.contains(new_highest_level)) {
            for (auto& candidate : node_levels_.at(new_highest_level)) {
                if (nodes_.contains(candidate) && nodes_.at(candidate).active) {
                    entry_point_ = candidate;
                    break;
                }
            }
            break;
        }
        new_highest_level--;
    }
    max_level_ = new_highest_level;
}

std::vector<Id> HNSWIndex::Search(const Vector& query, std::size_t k, std::size_t ef_search) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    if (!entry_point_.has_value()) {
        return {};
    }

    Id entry_point = entry_point_.value();

    for (int l = max_level_; l > 0; --l) {
        std::vector<Id> nearest = SearchLevel(query, entry_point, ef_search, l);

        if (nearest.empty()) {
            return {};
        }
        entry_point = nearest[0];
    }

    std::vector<Id> results = SearchLevel(query, entry_point, ef_search, 0);

    if (results.size() > k) {
        results.resize(k);
    }

    return results;
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

        if (top_ef.size() == ef) {
            if (distance > top_ef.top().first) {
                break;
            }
            top_ef.pop();
        }

        top_ef.emplace(std::pair<float, Id>(distance, node_id));

        if (!nodes_.at(node_id).neighbors.contains(level)) {
            continue;
        }

        for (Id neighbor : nodes_.at(node_id).neighbors.at(level)) {
            if (visited.contains(neighbor) || !nodes_.at(neighbor).active) {
                continue;
            }
            visited.insert(neighbor);
            float neighbor_distance = ComputeDistance(query, nodes_.at(neighbor).vector);
            candidates.emplace(std::pair<float, Id>(neighbor_distance, neighbor));
        }
    }

    std::vector<Id> nearest;

    while (!top_ef.empty()) {
        nearest.push_back(top_ef.top().second);
        top_ef.pop();
    }

    std::reverse(nearest.begin(), nearest.end());

    return nearest;
}

std::vector<Id> HNSWIndex::SelectNeighbors(const Vector& query, const std::vector<Id>& candidates, int level) const {
    std::vector<std::pair<float, Id>> candidate_distances;
    for (Id potential_neighbor : candidates) {
        if (!nodes_.at(potential_neighbor).active) {
            continue;
        }
        float distance = ComputeDistance(query, nodes_.at(potential_neighbor).vector);
        candidate_distances.emplace_back(distance, potential_neighbor);
    }
    std::sort(candidate_distances.begin(), candidate_distances.end());

    std::size_t max_neighbors = m_;
    if (level == 0) {
        max_neighbors = m0_;
    }

    std::vector<Id> assigned_neighbors;
    for (std::size_t i = 0; i < std::min(candidate_distances.size(), max_neighbors); ++i) {
        assigned_neighbors.push_back(candidate_distances.at(i).second);
    }

    return assigned_neighbors;
}

void HNSWIndex::Cleanup() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for (auto it = nodes_.begin(); it != nodes_.end();) {
        Id id = it->first;
        Node& node = it->second;

        if (!node.active) {
            it = nodes_.erase(it);
            continue;
        }

        for (auto& [level, neighbors] : node.neighbors) {
            for (auto neighbor_it = neighbors.begin(); neighbor_it != neighbors.end();) {
                if (!nodes_.contains(*neighbor_it) || !nodes_.at(*neighbor_it).active) {
                    neighbor_it = neighbors.erase(neighbor_it);
                    continue;
                }
                ++neighbor_it;
            }
        }

        ++it;
    }
}

void HNSWIndex::Reindex() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    std::vector<std::pair<Id, Vector>> active_vectors;
    active_vectors.reserve(nodes_.size());

    for (const auto& [id, node] : nodes_) {
        active_vectors.emplace_back(id, node.vector);
    }

    nodes_.clear();
    node_levels_.clear();
    entry_point_.reset();
    max_level_ = -1;

    for (const auto& [id, vector] : active_vectors) {
        Insert(id, vector);
    }
}

float HNSWIndex::ComputeDistance(const Vector& a, const Vector& b) const {
    if (a.size() != b.size()) {
        return std::numeric_limits<float>::infinity();
    }

    if (metric_ == DistanceMetric::L2) {
        float distance = 0.0f;

        for (size_t i = 0; i < a.size(); ++i) {
            float diff = a[i] - b[i];
            distance += diff * diff;
        }
        return std::sqrt(distance);
    }

    if (metric_ == DistanceMetric::Cosine) {
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

void HNSWIndex::ConnectNeighbors(Id node_id, const Vector& vector, const std::vector<Id>& neighbors, int level) {
    for (Id neighbor_id : neighbors) {
        if (!nodes_.at(neighbor_id).active) {
            continue;
        }

        nodes_[node_id].neighbors[level].insert(neighbor_id);
        nodes_[neighbor_id].neighbors[level].insert(node_id);

        auto& e_conn = nodes_[neighbor_id].neighbors[level];

        std::size_t max_neighbors = m_;
        if (level == 0) {
            max_neighbors = m0_;
        }

        if (e_conn.size() > max_neighbors) {
            std::vector<Id> candidates(e_conn.begin(), e_conn.end());
            std::vector<Id> e_new_conn = SelectNeighbors(nodes_[neighbor_id].vector, candidates, level);

            for (Id old_neighbor : e_conn) {
                if (std::find(e_new_conn.begin(), e_new_conn.end(), old_neighbor) == e_new_conn.end()) {
                    nodes_[old_neighbor].neighbors[level].erase(neighbor_id);
                }
            }

            e_conn.clear();
            for (Id new_neighbor : e_new_conn) {
                e_conn.insert(new_neighbor);
            }
        }
    }
}

int HNSWIndex::GetRandomLevel() const {
    return static_cast<int>(-std::log(1.0 - level_distribution_(random_engine_)) * ml_);
}

} // namespace vector_db_engine
