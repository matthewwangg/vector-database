#include "vector_store.h"

void VectorStore::Insert(Id id, const Vector& vector) {
    store_[id] = vector;
}

void VectorStore::Remove(Id id) {
    store_.erase(id);
}

void VectorStore::Search(const Vector& query, std::size_t k) {
    // unimplemented
}
