#include "vector_store.h"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace vector_db_engine {

VectorStore::VectorStore(IndexType index_type, std::unique_ptr<VectorIndex> index, int vector_dimensionality)
    : index_type_(index_type),
      index_(std::move(index)),
      vector_dimensionality_(vector_dimensionality)
{}

VectorStore::VectorStore(IndexType index_type, std::unique_ptr<VectorIndex> index, int vector_dimensionality, std::unordered_map<vector_db_engine::Id, Data> store)
    : index_type_(index_type),
      index_(std::move(index)),
      vector_dimensionality_(vector_dimensionality),
      store_(store)
{}

bool VectorStore::Insert(Id id, const Vector& vector, const std::string& content) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    if (store_.contains(id) || vector.size() != vector_dimensionality_) {
        return false;
    }

    store_[id] = Data{
        .id = id,
        .vector = vector,
        .content = content,
    };
    index_->Insert(id, vector);

    return true;
}

bool VectorStore::Remove(Id id) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    if (!store_.contains(id)) {
        return false;
    }

    store_.erase(id);
    index_->Remove(id);

    return true;
}

std::vector<VectorStore::Data> VectorStore::Search(const Vector& query, std::size_t k, std::size_t search_param) const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    if (query.size() != vector_dimensionality_) {
        return {};
    }

    std::vector<Id> result_ids = index_->Search(query, k, search_param);

    std::vector<Data> results;
    for (Id id : result_ids) {
        if (!store_.contains(id)) {
            continue;
        }
        results.push_back(store_.at(id));
    }

    return results;
}

void VectorStore::Cleanup(bool reindex) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    index_->Cleanup();

    if (reindex) {
        index_->Reindex();
    }
}

std::uint64_t VectorStore::Checksum() {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    std::uint64_t checksum = 0;
    for (const auto& [id, data] : store_) {
        checksum = checksum * 71 + id;

        for (float value : data.vector) {
            uint64_t bits;
            std::memcpy(&bits, &value, sizeof(value));
            checksum = checksum * 71 + bits;
        }

        for (unsigned char c : data.content) {
            checksum = checksum * 71 + c;
        }
    }

    return checksum;
}

} // namespace vector_db_engine