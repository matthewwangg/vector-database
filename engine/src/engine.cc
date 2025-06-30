#include "engine.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "vector_store.h"

namespace vector_db_engine {

Engine::Engine(std::unique_ptr<VectorIndex> index, int vector_dimensionality)
    : store_(std::make_unique<VectorStore>(std::move(index), vector_dimensionality))
{}

bool Engine::Insert(Id id, const Vector& vector, const std::string& content) {
    return store_->Insert(id, vector, content);
}

bool Engine::Remove(Id id) {
    return store_->Remove(id);
}

std::vector<VectorStore::Data> Engine::Search(const Vector& query, std::size_t k, std::size_t search_param) const {
    return store_->Search(query, k, search_param);
}

} // namespace vector_db_engine
