#ifndef VECTOR_DATABASE_VECTOR_STORE_H
#define VECTOR_DATABASE_VECTOR_STORE_H

#include <cstdint>
#include <unordered_map>
#include <vector>

using Id = std::uint64_t;
using Vector = std::vector<float>;

class VectorStore {
public:
    VectorStore() = default;

    void Insert(Id id, const Vector& vector);
    void Remove(Id id);
    std::vector<Id> Search(const Vector& query, std::size_t k);

private:
    std::unordered_map<Id, Vector> store_;
};

#endif //VECTOR_DATABASE_VECTOR_STORE_H
