#include "input_validator.h"

#include <cstdint>
#include <string>

#include "flat_index.h"
#include "hnsw_index.h"

namespace vector_db_engine {

bool ValidateInsert(std::string table_name, Id id, const Vector& vector, const std::string& content) {
    if (table_name.empty() || vector.empty() || content.empty()) {
        return false;
    }
    return true;
}

bool ValidateRemove(std::string table_name, Id id) {
    if (table_name.empty()) {
        return false;
    }
    return true;
}

bool ValidateSearch(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param) {
    if (table_name.empty() || query.empty() || k < 0) {
        return false;
    }
    return true;
}

bool ValidateStats(std::string table_name) {
    if (table_name.empty()) {
        return false;
    }
    return true;
}

bool ValidateMetrics(std::string table_name) {
    if (table_name.empty()) {
        return false;
    }
    return true;
}

bool ValidateCreateTable(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    if (name.empty() || vector_dimensionality == 0 || (hnsw_index_config.empty() && flat_index_config.empty()) || (!hnsw_index_config.empty() && !flat_index_config.empty())) {
        return false;
    }
    return true;
}

bool ValidateDropTable(std::string name) {
    if (name.empty()) {
        return false;
    }
    return true;
}

} // namespace vector_db_engine