#include "persistence_manager.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "flat_index.h"
#include "hnsw_index.h"
#include "logger.h"
#include "vector_store.h"

#include "replica.pb.h"
#include "storage.pb.h"

namespace vector_db_engine {

VectorPersistenceManager::VectorPersistenceManager(std::string name, StoredIndexType stored_index_type, std::string store_snapshot_file_path, std::string index_snapshot_file_path, std::string wal_file_path, std::string metadata_file_path, Logger* logger)
    : name_(name),
      stored_index_type_(stored_index_type),
      store_snapshot_file_path_(GetFullFilepath(store_snapshot_file_path)),
      index_snapshot_file_path_(GetFullFilepath(index_snapshot_file_path)),
      wal_file_path_(GetFullFilepath(wal_file_path)),
      metadata_file_path_(GetFullFilepath(metadata_file_path)),
      logger_(logger)
{
    const std::string base = std::string(std::getenv("HOME")) + "/.vector_db/" + name_ + "/";
    const std::string suffix = "_wal.log";
    table_name_ = wal_file_path_.substr(base.size(), wal_file_path_.size() - base.size() - suffix.size());

    wal_out_.open(wal_file_path_, std::ios::app);
    if (!wal_out_) {
        logger_->Error("failed to open write-ahead log", name_);
    }
}

void VectorPersistenceManager::SaveSnapshot(const VectorStore& store) const {
    vector_db::StoreSnapshot store_snapshot;
    store_snapshot.set_vector_dimensionality(store.GetVectorDimensionality());
    std::string index_type_string;

    for (auto &[id, data]: store.GetStore()) {
        auto *vector_entry = store_snapshot.add_vector_entry();
        vector_entry->set_id(id);
        vector_entry->set_content(data.content);

        for (float value: data.vector) {
            vector_entry->add_vector(value);
        }
    }

    std::ofstream out_store(store_snapshot_file_path_, std::ios::binary);
    if (!out_store) {
        logger_->Error("failed to open store file", name_);
        return;
    }
    store_snapshot.SerializeToOstream(&out_store);

    if (stored_index_type_ == StoredIndexType::HNSW) {
        index_type_string = "HNSW";
        const auto* index = dynamic_cast<const HNSWIndex*>(store.GetIndex());
        if (!index) {
            logger_->Error("failed to get index, skipping index save", name_);
            return;
        }

        vector_db::HNSWIndexSnapshot index_snapshot;
        index_snapshot.set_m(index->GetConfig().m);
        index_snapshot.set_m0(index->GetConfig().m0);
        index_snapshot.set_ef_construction(index->GetConfig().ef_construction);
        index_snapshot.set_ml(index->GetConfig().ml);
        index_snapshot.set_vector_dimensionality(index->GetConfig().vector_dimensionality);
        index_snapshot.set_max_level(index->GetMaxLevel());

        if (index->GetEntryPoint().has_value()) {
            index_snapshot.set_entry_point(index->GetEntryPoint().value());
        }

        if (index->GetConfig().metric == VectorIndex::DistanceMetric::L2) {
            index_snapshot.set_distance_metric(vector_db::L2);
        }
        if (index->GetConfig().metric == VectorIndex::DistanceMetric::Cosine) {
            index_snapshot.set_distance_metric(vector_db::COSINE);
        }

        for (const auto &[id, node]: index->GetNodes()) {
            vector_db::Node storage_node;
            storage_node.set_level(node.level);
            storage_node.set_active(node.active);

            for (float value: node.vector) {
                storage_node.add_vector(value);
            }

            for (const auto &[level, ids]: node.neighbors) {
                vector_db::IdSet id_set;
                for (Id neighbor_id: ids) {
                    id_set.add_id(neighbor_id);
                }
                (*storage_node.mutable_neighbors())[level] = std::move(id_set);
            }
            (*index_snapshot.mutable_nodes())[id] = std::move(storage_node);
        }

        for (const auto &[level, ids]: index->GetNodeLevels()) {
            vector_db::IdSet id_set;
            for (Id id: ids) {
                id_set.add_id(id);
            }
            (*index_snapshot.mutable_node_levels())[level] = std::move(id_set);
        }

        std::ofstream out_index(index_snapshot_file_path_, std::ios::binary);
        if (!out_index) {
            logger_->Error("failed to open index file", name_);
            return;
        }
        index_snapshot.SerializeToOstream(&out_index);
    } else {
        index_type_string = "flat";
        const auto* index = dynamic_cast<const FlatIndex*>(store.GetIndex());
        if (!index) {
            logger_->Error("failed to get index, skipping index save", name_);
            return;
        }

        vector_db::FlatIndexSnapshot index_snapshot;
        index_snapshot.set_vector_dimensionality(index->GetConfig().vector_dimensionality);
        if (index->GetConfig().metric == VectorIndex::DistanceMetric::L2) {
            index_snapshot.set_distance_metric(vector_db::L2);
        }
        if (index->GetConfig().metric == VectorIndex::DistanceMetric::Cosine) {
            index_snapshot.set_distance_metric(vector_db::COSINE);
        }

        for (const auto& [id, vector] : index->GetVectors()) {
            auto& vector_entry = (*index_snapshot.mutable_vectors())[id];
            for (float value : vector) {
                vector_entry.add_vector(value);
            }
        }

        std::ofstream out_index(index_snapshot_file_path_, std::ios::binary);
        if (!out_index) {
            logger_->Error("failed to open index file", name_);
            return;
        }
        index_snapshot.SerializeToOstream(&out_index);
    }

    if (!std::filesystem::exists(index_snapshot_file_path_) ||
        std::filesystem::file_size(index_snapshot_file_path_) == 0) {
        return;
    }

    logger_->Info(table_name_ + " snapshot saved successfully with " + index_type_string + " index", name_);
}

std::unique_ptr<VectorStore> VectorPersistenceManager::LoadSnapshot() const {
    VectorStore::IndexType index_type;
    std::unique_ptr<VectorIndex> reconstructed_index;
    std::string index_type_string;

    if (stored_index_type_ == StoredIndexType::HNSW) {
        index_type_string = "HNSW";
        index_type = VectorStore::IndexType::HNSW;

        vector_db::HNSWIndexSnapshot index_snapshot;
        std::ifstream in_index(index_snapshot_file_path_, std::ios::binary);
        if (!in_index) {
            return nullptr;
        }
        index_snapshot.ParseFromIstream(&in_index);

        std::size_t m = index_snapshot.m();
        std::size_t m0 = index_snapshot.m0();
        std::size_t ef_construction = index_snapshot.ef_construction();
        float ml = index_snapshot.ml();
        int vector_dimensionality = index_snapshot.vector_dimensionality();
        int max_level = index_snapshot.max_level();
        std::optional<Id> entry_point = index_snapshot.entry_point();

        VectorIndex::DistanceMetric distance_metric;
        if (index_snapshot.distance_metric() == vector_db::L2) {
            distance_metric = VectorIndex::DistanceMetric::L2;
        }
        if (index_snapshot.distance_metric() == vector_db::COSINE) {
            distance_metric = VectorIndex::DistanceMetric::Cosine;
        }

        std::unordered_map<Id, HNSWIndex::Node> reconstructed_nodes;
        for (const auto &[id, storage_node]: index_snapshot.nodes()) {
            HNSWIndex::Node node;
            node.level = storage_node.level();
            node.active = storage_node.active();
            node.vector.assign(storage_node.vector().begin(), storage_node.vector().end());

            for (const auto &[level, ids]: storage_node.neighbors()) {
                std::unordered_set neighbor_ids(ids.id().begin(), ids.id().end());
                node.neighbors[level] = std::move(neighbor_ids);
            }

            reconstructed_nodes[id] = std::move(node);
        }

        std::unordered_map<int, std::unordered_set<Id>> reconstructed_node_levels;
        for (const auto &[level, ids]: index_snapshot.node_levels()) {
            std::unordered_set<Id> id_set(ids.id().begin(), ids.id().end());
            reconstructed_node_levels[level] = std::move(id_set);
        }

        HNSWIndex::HNSWIndexConfig config = {m, m0, ef_construction, ml, distance_metric, vector_dimensionality};
        reconstructed_index = std::make_unique<HNSWIndex>(config, max_level, entry_point, reconstructed_nodes,
                                                                                     reconstructed_node_levels);
    } else {
        index_type_string = "flat";
        index_type = VectorStore::IndexType::FLAT;

        vector_db::FlatIndexSnapshot index_snapshot;
        std::ifstream in_index(index_snapshot_file_path_, std::ios::binary);
        if (!in_index) {
            return nullptr;
        }
        index_snapshot.ParseFromIstream(&in_index);

        int vector_dimensionality = index_snapshot.vector_dimensionality();

        VectorIndex::DistanceMetric distance_metric;
        if (index_snapshot.distance_metric() == vector_db::L2) {
            distance_metric = VectorIndex::DistanceMetric::L2;
        }
        if (index_snapshot.distance_metric() == vector_db::COSINE) {
            distance_metric = VectorIndex::DistanceMetric::Cosine;
        }

        std::unordered_map<Id, Vector> vectors;
        for (const auto& [id, vector_proto] : index_snapshot.vectors()) {
            Vector vector(vector_proto.vector().begin(), vector_proto.vector().end());
            vectors[id] = std::move(vector);
        }

        FlatIndex::FlatIndexConfig config = {vector_dimensionality, distance_metric};
        reconstructed_index = std::make_unique<FlatIndex>(config, vectors);
    }
    vector_db::StoreSnapshot store_snapshot;
    std::ifstream in_store(store_snapshot_file_path_, std::ios::binary);
    if (!in_store) {
        return nullptr;
    }
    store_snapshot.ParseFromIstream(&in_store);

    std::unordered_map<Id, VectorStore::Data> reconstructed_store;
    for (const auto &vector_entry: store_snapshot.vector_entry()) {
        Vector vector(vector_entry.vector().begin(), vector_entry.vector().end());
        Id id = vector_entry.id();
        reconstructed_store[id] = {id, vector, vector_entry.content()};
    }

    std::unique_ptr<VectorStore> loaded_store = std::make_unique<VectorStore>(index_type,
                                                                              std::move(reconstructed_index),
                                                                              store_snapshot.vector_dimensionality(),
                                                                              reconstructed_store);
    if (loaded_store->GetStore().size() == 0) {
        return nullptr;
    }

    logger_->Info(table_name_ + " snapshot loaded successfully with " + index_type_string + " index, " +
                  std::to_string(store_snapshot.vector_entry_size()) + " vectors, and dimensionality of " +
                  std::to_string(store_snapshot.vector_dimensionality()), name_);

    return std::move(loaded_store);
}

void VectorPersistenceManager::SaveMetadata(int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) const {
    vector_db::TableMetadata table_metadata;
    table_metadata.set_table_name(table_name_);
    table_metadata.set_vector_dimensionality(vector_dimensionality);
    table_metadata.set_cache_size(cache_size);

    if (hnsw_index_config.m != 0 && hnsw_index_config.m0 != 0 && hnsw_index_config.ef_construction != 0 && hnsw_index_config.vector_dimensionality != 0) {
        table_metadata.set_index_type(vector_db::TableMetadata::HNSW);
        table_metadata.mutable_hnsw_index_config()->set_m(hnsw_index_config.m);
        table_metadata.mutable_hnsw_index_config()->set_m0(hnsw_index_config.m0);
        table_metadata.mutable_hnsw_index_config()->set_ef_construction(hnsw_index_config.ef_construction);
        table_metadata.mutable_hnsw_index_config()->set_ml(hnsw_index_config.ml);
        table_metadata.mutable_hnsw_index_config()->set_distance_metric(static_cast<vector_db::DistanceMetric>(hnsw_index_config.metric));
        table_metadata.mutable_hnsw_index_config()->set_vector_dimensionality(hnsw_index_config.vector_dimensionality);
    } else {
        table_metadata.set_index_type(vector_db::TableMetadata::FLAT);
        table_metadata.mutable_flat_index_config()->set_distance_metric(static_cast<vector_db::DistanceMetric>(flat_index_config.metric));
        table_metadata.mutable_flat_index_config()->set_vector_dimensionality(flat_index_config.vector_dimensionality);
    }

    std::ofstream out(metadata_file_path_, std::ios::binary);
    table_metadata.SerializeToOstream(&out);
}

void VectorPersistenceManager::AppendInsert(Id id, const Vector& vector, const std::string& content) {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);
    wal_out_ << "insert " << id;
    for (float value : vector) {
        wal_out_ << " " << value;
    }
    wal_out_ << " | " << content;
    wal_out_ << "\n";
    wal_out_.flush();
}

void VectorPersistenceManager::AppendRemove(Id id) {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);
    wal_out_ << "remove " << id << "\n";
    wal_out_.flush();
}

void VectorPersistenceManager::AppendCreate(const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);
    if (stored_index_type_ == StoredIndexType::HNSW) {
        wal_out_ << "create " << hnsw_index_config.vector_dimensionality << " " <<  hnsw_index_config.m << " " << hnsw_index_config.m0 << " " << hnsw_index_config.ef_construction << " " << hnsw_index_config.ml << " " << (hnsw_index_config.metric == VectorIndex::DistanceMetric::L2 ? "L2" : "COSINE") << " " << cache_size << "\n";
    } else {
        wal_out_ << "create " << flat_index_config.vector_dimensionality << " " << (flat_index_config.metric == VectorIndex::DistanceMetric::L2 ? "L2" : "COSINE") << " " << cache_size << "\n";
    }
    wal_out_.flush();
}

void VectorPersistenceManager::AppendDrop() {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);
    wal_out_ << "drop" << "\n";
    wal_out_.flush();
}

void VectorPersistenceManager::ReplayWAL(const std::function<void(const vector_db::WALEntry&)>& callback) {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);

    bool replay = false;

    std::ifstream wal_in(wal_file_path_);
    if (!wal_in) {
        return;
    }

    std::string line;
    while (std::getline(wal_in, line)) {
        vector_db::WALEntry entry;
        entry.set_table(table_name_);
        replay = true;

        std::istringstream stream(line);
        std::string command;
        stream >> command;

        if (command == "insert") {
            entry.set_type(vector_db::WALEntry::INSERT);

            Id id;
            stream >> id;
            entry.mutable_insert_config()->set_id(id);

            float value;
            while (stream >> value)  {
                entry.mutable_insert_config()->add_vector(value);
            }

            stream.clear();
            stream >> std::ws;

            std::string content;
            std::getline(stream, content);
            if (content.starts_with("| ")) {
                content = content.substr(2);
            }
            entry.mutable_insert_config()->set_content(content);
        }
        if (command == "remove") {
            entry.set_type(vector_db::WALEntry::REMOVE);

            Id id;
            stream >> id;
            entry.mutable_remove_config()->set_id(id);
        }
        if (command == "create") {
            entry.set_type(vector_db::WALEntry::CREATE);

            int vector_dimensionality;
            std::string distance_metric_string;
            std::size_t cache_size;

            if (stored_index_type_ == StoredIndexType::HNSW) {
                std::size_t m;
                std::size_t m0;
                std::size_t ef_construction;
                float ml;

                stream >> vector_dimensionality;
                stream >> m;
                stream >> m0;
                stream >> ef_construction;
                stream >> ml;
                stream >> distance_metric_string;
                stream >> cache_size;

                entry.mutable_create_config()->mutable_store_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_m(m);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_m0(m0);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_ef_construction(ef_construction);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_ml(ml);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_distance_metric(
                        (distance_metric_string == "L2" ? vector_db::WALEntry::CreateConfig::L2
                                                        : vector_db::WALEntry::CreateConfig::COSINE));
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_cache_config()->set_cache_size(cache_size);
            } else {
                stream >> vector_dimensionality;
                stream >> distance_metric_string;
                stream >> cache_size;

                entry.mutable_create_config()->mutable_store_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_flat_index_config()->set_distance_metric(
                        (distance_metric_string == "L2" ? vector_db::WALEntry::CreateConfig::L2
                                                        : vector_db::WALEntry::CreateConfig::COSINE));
                entry.mutable_create_config()->mutable_flat_index_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_cache_config()->set_cache_size(cache_size);
            }
        }
        if (command == "drop") {
            entry.set_type(vector_db::WALEntry::DROP);
        }
        callback(entry);
    }

    if (!replay) {
        return;
    }

    logger_->Info(table_name_ + " write-ahead log replay completed", name_);
}

void VectorPersistenceManager::ClearWAL() {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);

    bool clear = std::filesystem::exists(wal_file_path_) && std::filesystem::file_size(wal_file_path_) > 0;
    std::ofstream clear_log(wal_file_path_, std::ios::trunc);

    if (!clear) {
        return;
    }

    logger_->Info(table_name_ + " write-ahead log cleared", name_);
}

void VectorPersistenceManager::Clear() {
    std::filesystem::remove(store_snapshot_file_path_);
    std::filesystem::remove(index_snapshot_file_path_);
    std::filesystem::remove(wal_file_path_);
    std::filesystem::remove(metadata_file_path_);
}

std::vector<vector_db::WALEntry> VectorPersistenceManager::SerializeWALEntries(int offset) {
    std::lock_guard<std::mutex> lock(wal_log_mutex_);

    std::vector<vector_db::WALEntry> entries;

    std::ifstream wal_in(wal_file_path_);
    if (!wal_in) {
        return {};
    }

    std::string line;
    int current_line = 0;
    while (std::getline(wal_in, line)) {
        if (current_line < offset) {
            current_line++;
            continue;
        }

        vector_db::WALEntry entry;
        entry.set_table(table_name_);

        std::istringstream stream(line);
        std::string command;
        stream >> command;

        if (command == "insert") {
            entry.set_type(vector_db::WALEntry::INSERT);

            Id id;
            stream >> id;
            entry.mutable_insert_config()->set_id(id);

            float value;
            while (stream >> value)  {
                entry.mutable_insert_config()->add_vector(value);
            }

            stream.clear();
            stream >> std::ws;

            std::string content;
            std::getline(stream, content);
            if (content.starts_with("| ")) {
                content = content.substr(2);
            }
            entry.mutable_insert_config()->set_content(content);
        }
        if (command == "remove") {
            entry.set_type(vector_db::WALEntry::REMOVE);

            Id id;
            stream >> id;
            entry.mutable_remove_config()->set_id(id);
        }
        if (command == "create")  {
            entry.set_type(vector_db::WALEntry::CREATE);
            int vector_dimensionality;
            std::string distance_metric_string;
            std::size_t cache_size;

            if (stored_index_type_ == StoredIndexType::HNSW) {
                std::size_t m;
                std::size_t m0;
                std::size_t ef_construction;
                float ml;

                stream >> vector_dimensionality;
                stream >> m;
                stream >> m0;
                stream >> ef_construction;
                stream >> ml;
                stream >> distance_metric_string;
                stream >> cache_size;

                entry.mutable_create_config()->mutable_store_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_m(m);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_m0(m0);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_ef_construction(ef_construction);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_ml(ml);
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_distance_metric(
                        (distance_metric_string == "L2" ? vector_db::WALEntry::CreateConfig::L2
                                                        : vector_db::WALEntry::CreateConfig::COSINE));
                entry.mutable_create_config()->mutable_hnsw_index_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_cache_config()->set_cache_size(cache_size);
            } else {
                stream >> vector_dimensionality;
                stream >> distance_metric_string;
                stream >> cache_size;

                entry.mutable_create_config()->mutable_store_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_flat_index_config()->set_distance_metric(
                        (distance_metric_string == "L2" ? vector_db::WALEntry::CreateConfig::L2
                                                        : vector_db::WALEntry::CreateConfig::COSINE));
                entry.mutable_create_config()->mutable_flat_index_config()->set_vector_dimensionality(vector_dimensionality);
                entry.mutable_create_config()->mutable_cache_config()->set_cache_size(cache_size);
            }
        }
        if (command == "drop") {
            entry.set_type(vector_db::WALEntry::DROP);
        }

        entries.push_back(entry);
        current_line++;
    }

    return entries;
}

std::string VectorPersistenceManager::GetFullFilepath(std::string file_path) {
    const char* home = std::getenv("HOME");
    if (!home) {
        return file_path;
    }

    std::filesystem::create_directories(std::filesystem::path(home) / ".vector_db" / name_);

    return std::string(home) + "/.vector_db/" + name_ + "/" + file_path;
}

} // namespace vector_db_engine
