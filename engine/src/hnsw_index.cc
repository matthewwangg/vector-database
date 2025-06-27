#include "hnsw_index.h"

HNSWIndex::HNSWIndex(std::size_t m, std::size_t ef_construction, float ml)
    : m_(m),
    ef_construction_(ef_construction),
    ml_(ml),
    max_level_(-1),
    random_engine_(std::random_device{}()),
    level_distribution_(0.0, 1.0)
{}

void HNSWIndex::Insert(Id id, const Vector& vector) {

}
void HNSWIndex::Remove(Id id) {

}

std::vector<Id> HNSWIndex::Search(const Vector& query, std::size_t k) const {
    return {};
}

int HNSWIndex::GetRandomLevel() const {
    return 0;
}
