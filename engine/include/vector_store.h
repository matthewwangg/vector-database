#ifndef VECTOR_DATABASE_VECTOR_STORE_H
#define VECTOR_DATABASE_VECTOR_STORE_H

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorStore {
public:
    struct Data {
        Vector vector;
        std::string content;
    };

    explicit VectorStore(std::unique_ptr<VectorIndex> index, int vector_dimensionality_);

    bool Insert(Id id, const Vector& vector, const std::string& content);

    bool Remove(Id id);

    std::vector<Data> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

private:
    std::unordered_map<Id, Data> store_;
    std::unique_ptr<VectorIndex> index_;

    int vector_dimensionality_;

    mutable std::shared_mutex rw_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_VECTOR_STORE_H
