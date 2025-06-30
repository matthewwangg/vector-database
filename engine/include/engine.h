#ifndef VECTOR_DATABASE_ENGINE_H
#define VECTOR_DATABASE_ENGINE_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "vector_store.h"

namespace vector_db_engine {

class Engine {
public:
    explicit Engine(std::unique_ptr<VectorIndex> index, int vector_dimensionality);

    bool Insert(Id id, const Vector& vector, const std::string& content);

    bool Remove(Id id);

    std::vector<VectorStore::Data> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

private:
    std::unique_ptr<VectorStore> store_;
};

}

#endif //VECTOR_DATABASE_ENGINE_H
