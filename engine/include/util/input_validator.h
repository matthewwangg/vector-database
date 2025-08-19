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

// InputValidator is responsible for taking in the input of the core vector database operations on the engine interface. It determines whether those inputs are valid for use.
class InputValidator {
public:
    InputValidator() = default;

    // Validate the input of the insert operation on the engine interface.
    bool ValidateInsert(const std::string& table_name, Id id, const Vector& vector, const std::string& content);

    // Validate the input of the remove operation on the engine interface.
    bool ValidateRemove(const std::string& table_name, Id id);

    // Validate the input of the search operation on the engine interface.
    bool ValidateSearch(const std::string& table_name, const Vector& query, std::size_t k, std::size_t search_param);

    // Validate the input of the get stats operation on the engine interface.
    bool ValidateStats(const std::string& table_name);

    // Validate the input of the get metrics operation on the engine interface.
    bool ValidateMetrics(const std::string& table_name);

    // Validate the input of the create table operation on the engine interface.
    bool ValidateCreateTable(const std::string& name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);

    // Validate the input of the drop table operation on the engine interface.
    bool ValidateDropTable(const std::string& name);
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_INPUT_VALIDATOR_H
