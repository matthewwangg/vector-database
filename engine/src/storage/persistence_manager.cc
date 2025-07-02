#include "persistence_manager.h"

#include <fstream>
#include <memory>
#include <string>

#include "hnsw_index.h"
#include "vector_store.h"

#include "storage.pb.h"

namespace vector_db_engine {

VectorPersistenceManager::VectorPersistenceManager(std::string store_snapshot_file_path, std::string index_snapshot_file_path)
    : store_snapshot_file_path_(store_snapshot_file_path),
      index_snapshot_file_path_(index_snapshot_file_path)
{}

void VectorPersistenceManager::SaveSnapshot(const VectorStore& store) const {
    vector_db::StoreSnapshot store_snapshot;
    store_snapshot.set_vector_dimensionality(store.GetVectorDimensionality());

    for (auto& [id, data] : store.GetStore()) {
        auto* vector_entry = store_snapshot.add_vector_entry();
        vector_entry->set_id(id);
        vector_entry->set_content(data.content);

        for (float value : data.vector) {
            vector_entry->add_vector(value);
        }
    }

    std::ofstream out_store(store_snapshot_file_path_, std::ios::binary);
    if (!out_store) {
        std::cout << "failed to open store file" << std::endl;
        return;
    }
    store_snapshot.SerializeToOstream(&out_store);

    const auto* index = dynamic_cast<const HNSWIndex*>(store.GetIndex());
    if (!index) {
        std::cout << "failed to get index, skipping index save" << std::endl;
        return;
    }

    vector_db::IndexSnapshot index_snapshot;
    index_snapshot.set_m(index->GetM());
    index_snapshot.set_m0(index->GetM0());
    index_snapshot.set_ef_construction(index->GetEfConstruction());
    index_snapshot.set_ml(index->GetML());
    index_snapshot.set_vector_dimensionality(index->GetVectorDimensionality());
    index_snapshot.set_max_level(index->GetMaxLevel());

    if (index->GetEntryPoint().has_value()) {
        index_snapshot.set_entry_point(index->GetEntryPoint().value());
    }

    if (index->GetMetric() == HNSWIndex::DistanceMetric::L2) {
        index_snapshot.set_distance_metric(vector_db::IndexSnapshot::L2);
    }
    if (index->GetMetric() == HNSWIndex::DistanceMetric::Cosine) {
        index_snapshot.set_distance_metric(vector_db::IndexSnapshot::COSINE);
    }

    for (const auto& [id, node] : index->GetNodes()) {
        vector_db::Node storage_node;
        storage_node.set_level(node.level);
        storage_node.set_active(node.active);

        for (float value : node.vector) {
            storage_node.add_vector(value);
        }

        for (const auto& [level, ids] : node.neighbors) {
            vector_db::IdSet id_set;
            for (Id neighbor_id : ids) {
                id_set.add_id(neighbor_id);
            }
            (*storage_node.mutable_neighbors())[level] = std::move(id_set);
        }
        (*index_snapshot.mutable_nodes())[id] = std::move(storage_node);
    }

    for (const auto& [level, ids] : index->GetNodeLevels()) {
        vector_db::IdSet id_set;
        for (Id id : ids) {
            id_set.add_id(id);
        }
        (*index_snapshot.mutable_node_levels())[level] = std::move(id_set);
    }

    std::ofstream out_index(index_snapshot_file_path_, std::ios::binary);
    if (!out_index) {
        std::cout << "failed to open index file" << std::endl;
        return;
    }
    index_snapshot.SerializeToOstream(&out_index);

    std::cout << "snapshot saved successfully" << std::endl;
}

std::unique_ptr<VectorStore> VectorPersistenceManager::LoadSnapshot() const {
    vector_db::IndexSnapshot index_snapshot;
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

    HNSWIndex::DistanceMetric distance_metric;
    if (index_snapshot.distance_metric() == vector_db::IndexSnapshot::L2) {
        distance_metric = HNSWIndex::DistanceMetric::L2;
    }
    if (index_snapshot.distance_metric() == vector_db::IndexSnapshot::COSINE) {
        distance_metric = HNSWIndex::DistanceMetric::Cosine;
    }

    std::unordered_map<Id, HNSWIndex::Node> reconstructed_nodes;
    for (const auto& [id, storage_node] : index_snapshot.nodes()) {
        HNSWIndex::Node node;
        node.level = storage_node.level();
        node.active = storage_node.active();
        node.vector.assign(storage_node.vector().begin(), storage_node.vector().end());

        for (const auto& [level, ids] : storage_node.neighbors()) {
            std::unordered_set neighbor_ids(ids.id().begin(), ids.id().end());
            node.neighbors[level] = std::move(neighbor_ids);
        }

        reconstructed_nodes[id] = std::move(node);
    }

    std::unordered_map<int, std::unordered_set<Id>> reconstructed_node_levels;
    for (const auto& [level, ids] : index_snapshot.node_levels()) {
        std::unordered_set<Id> id_set(ids.id().begin(), ids.id().end());
        reconstructed_node_levels[level] = std::move(id_set);
    }

    std::unique_ptr<HNSWIndex> reconstructed_index = std::make_unique<HNSWIndex>(m, m0, ef_construction, ml, distance_metric, vector_dimensionality, max_level, entry_point, reconstructed_nodes, reconstructed_node_levels);

    vector_db::StoreSnapshot store_snapshot;
    std::ifstream in_store(store_snapshot_file_path_, std::ios::binary);
    if (!in_store) {
        return nullptr;
    }
    store_snapshot.ParseFromIstream(&in_store);

    std::unordered_map<Id, VectorStore::Data> reconstructed_store;
    for (const auto& vector_entry : store_snapshot.vector_entry()) {
        Vector vector(vector_entry.vector().begin(), vector_entry.vector().end());
        Id id = vector_entry.id();
        reconstructed_store[id] = { vector, vector_entry.content() };
    }

    std::unique_ptr<VectorStore> loaded_store = std::make_unique<VectorStore>(std::move(reconstructed_index), store_snapshot.vector_dimensionality(), reconstructed_store);

    std::cout << "snapshot loaded successfully with " << store_snapshot.vector_entry_size() << " vectors, " << index_snapshot.nodes_size() << " index nodes, and dimensionality of " << store_snapshot.vector_dimensionality() << std::endl;

    return std::move(loaded_store);
}

} // namespace vector_db_engine
