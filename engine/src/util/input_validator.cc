#include "input_validator.h"

#include <cstdint>
#include <string>

#include "flat_index.h"
#include "hnsw_index.h"

namespace vector_db_engine {

bool ValidateInsert(std::string table_name, Id id, const Vector& vector, const std::string& content) {
    return true;
}

bool ValidateRemove(std::string table_name, Id id) {
    return true;
}

bool ValidateSearch(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param) {
    return true;
}

bool ValidateStats(std::string table_name) {
    return true;
}

bool ValidateMetrics(std::string table_name) {
    return true;
}

bool ValidateCreateTable(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    return true;
}

bool ValidateDropTable(std::string name) {
    return true;
}

} // namespace vector_db_engine