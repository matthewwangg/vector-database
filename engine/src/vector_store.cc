#include "vector_store.h"

namespace vector_db_engine {

VectorStore::VectorStore(std::unique_ptr<VectorIndex> index, int vector_dimensionality)
    : index_(std::move(index)),
      vector_dimensionality_(vector_dimensionality)
{}

void VectorStore::Insert(Id id, const Vector& vector, const std::string& content) {
    store_[id] = Data{
        .vector = vector,
        .content = content,
    };
    index_->Insert(id, vector);
}

void VectorStore::Remove(Id id) {
    store_.erase(id);
    index_->Remove(id);
}

std::vector<Data> VectorStore::Search(const Vector& query, std::size_t k) const {
    std::vector<Id> result_ids = index_->Search(query, k, 32);

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