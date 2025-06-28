#include "vector_store.h"

namespace vector_db_engine {

void VectorStore::Insert(Id id, const Vector& vector) {
    store_[id] = vector;
    index_->Insert(id, vector);
}

void VectorStore::Remove(Id id) {
    store_.erase(id);
    index_->Remove(id);
}

std::vector<Id> VectorStore::Search(const Vector& query, std::size_t k) const {
    return index_->Search(query, k, 32);
}

} // namespace vector_db_engine