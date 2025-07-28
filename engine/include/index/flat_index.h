#ifndef VECTOR_DATABASE_FLAT_INDEX_H
#define VECTOR_DATABASE_FLAT_INDEX_H

#include <cstdint>
#include <vector>

#include "vector_index.h"

namespace vector_db_engine {

class FlatIndex : public VectorIndex {
public:
    FlatIndex();

    void Insert(Id id, const Vector& vector);
    void Remove(Id id);
    std::vector<Id> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

    void Cleanup();
    void Reindex();
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_FLAT_INDEX_H
