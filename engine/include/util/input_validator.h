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
    bool ValidateInsert(std::string table_name, Id id, const Vector& vector, const std::string& content);
    bool ValidateRemove(std::string table_name, Id id);
    bool ValidateSearch(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param);
    bool ValidateStats(std::string table_name);
    bool ValidateMetrics(std::string table_name);
    bool ValidateCreateTable(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);
    bool ValidateDropTable(std::string name);
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_INPUT_VALIDATOR_H
