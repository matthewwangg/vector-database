#ifndef VECTOR_DATABASE_INPUT_VALIDATOR_H
#define VECTOR_DATABASE_INPUT_VALIDATOR_H

#include <cstdint>
#include <string>
#include <vector>

#include "flat_index.h"
#include "hnsw_index.h"

namespace vector_db_engine {

using Id = std::uint64_t;
using Vector = std::vector<float>;

class InputValidator {
public:
    InputValidator() = default;

    bool ValidateInsert(const std::string& table_name, Id id, const Vector& vector, const std::string& content);
    bool ValidateRemove(const std::string& table_name, Id id);
    bool ValidateSearch(const std::string& table_name, const Vector& query, std::size_t k, std::size_t search_param);
    bool ValidateStats(const std::string& table_name);
    bool ValidateMetrics(const std::string& table_name);
    bool ValidateCreateTable(const std::string& name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);
    bool ValidateDropTable(const std::string& name);
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_INPUT_VALIDATOR_H
