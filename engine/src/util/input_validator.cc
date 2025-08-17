#include "input_validator.h"

#include <cstdint>
#include <string>

#include "flat_index.h"
#include "hnsw_index.h"

namespace vector_db_engine {

bool InputValidator::ValidateInsert(const std::string& table_name, Id id, const Vector& vector, const std::string& content) {
    if (table_name.empty() || vector.empty() || content.empty()) {
        return false;
    }
    return true;
}

bool InputValidator::ValidateRemove(const std::string& table_name, Id id) {
    if (table_name.empty()) {
        return false;
    }
    return true;
}

bool InputValidator::ValidateSearch(const std::string& table_name, const Vector& query, std::size_t k, std::size_t search_param) {
    if (table_name.empty() || query.empty() || k == 0) {
        return false;
    }
    return true;
}

bool InputValidator::ValidateStats(const std::string& table_name) {
    if (table_name.empty()) {
        return false;
    }
    return true;
}

bool InputValidator::ValidateMetrics(const std::string& table_name) {
    if (table_name.empty()) {
        return false;
    }
    return true;
}

bool InputValidator::ValidateCreateTable(const std::string& name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    if (name.empty() || vector_dimensionality == 0) {
        return false;
    }

    bool is_hnsw_index_config_empty = hnsw_index_config.vector_dimensionality == 0 && hnsw_index_config.m == 0 && hnsw_index_config.m0 == 0 && hnsw_index_config.ef_construction == 0 && hnsw_index_config.ml == 0.0f;
    bool is_flat_index_config_empty = flat_index_config.vector_dimensionality == 0;
    if ((is_hnsw_index_config_empty && is_flat_index_config_empty) || (!is_hnsw_index_config_empty && !is_flat_index_config_empty)) {
        return false;
    }

    bool is_hnsw_index_config_valid = hnsw_index_config.vector_dimensionality != 0 && hnsw_index_config.m != 0 && hnsw_index_config.m0 != 0 && hnsw_index_config.ef_construction != 0 && hnsw_index_config.ml != 0.0f;
    bool is_flat_index_config_valid = flat_index_config.vector_dimensionality != 0;

    if ((!is_hnsw_index_config_empty && !is_hnsw_index_config_valid) || (!is_flat_index_config_empty &&!is_flat_index_config_valid)) {
        return false;
    }

    return true;
}

bool InputValidator::ValidateDropTable(const std::string& name) {
    if (name.empty()) {
        return false;
    }
    return true;
}

} // namespace vector_db_engine