#include "vector_store.h"

namespace vector_db_engine {

VectorStore::VectorStore(std::unique_ptr<VectorIndex> index, int vector_dimensionality)
    : index_(std::move(index)), vector_dimensionality_(vector_dimensionality)
{}

bool VectorStore::Insert(Id id, const Vector& vector, const std::string& content) {
    if (store_.contains(id) || vector.size() != vector_dimensionality_) {
        return false;
    }

    store_[id] = Data{
        .vector = vector,
        .content = content,
    };
    index_->Insert(id, vector);

    return true;
}

bool VectorStore::Remove(Id id) {
    if (!store_.contains(id)) {
        return false;
    }

    store_.erase(id);
    index_->Remove(id);

    return true;
}

std::vector<VectorStore::Data> VectorStore::Search(const Vector& query, std::size_t k, std::size_t search_param) const {
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

} // namespace vector_db_engine