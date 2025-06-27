#include "vector_store.h"

namespace vector_db_engine {

void VectorStore::Insert(Id id, const Vector& vector) {
    store_[id] = vector;
}

void VectorStore::Remove(Id id) {
    store_.erase(id);
}

std::vector<Id> VectorStore::Search(const Vector& query, std::size_t k) const {
    return {};
}

} // namespace vector_db_engine