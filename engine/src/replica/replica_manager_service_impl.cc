#include "replica_manager_service_impl.h"

#include <grpcpp/grpcpp.h>

#include "replica.grpc.pb.h"
#include "replica.pb.h"

namespace vector_db_engine {

ReplicaManagerServiceImpl::ReplicaManagerServiceImpl(ReplicaManager* replica_manager)
    : replica_manager_(replica_manager)
{}

grpc::Status ReplicaManagerServiceImpl::Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) {
    for (const auto& entry : request->entry()) {
        replica_manager_->ApplyWALEntry(entry);
    }
    return grpc::Status::OK;
}

grpc::Status ReplicaManagerServiceImpl::Create(grpc::ServerContext* context, const vector_db::CreateRequest* request, vector_db::CreateResponse* response) {
    std::string name = request->table();
    int vector_dimensionality = request->store_config().vector_dimensionality();
    std::size_t m = request->hnsw_index_config().m();
    std::size_t m0 = request->hnsw_index_config().m0();
    std::size_t ef_construction = request->hnsw_index_config().ef_construction();
    float ml = request->hnsw_index_config().ml();
    std::size_t cache_size = request->cache_config().cache_size();

    HNSWIndex::DistanceMetric distance_metric;
    if (request->hnsw_index_config().distance_metric() == vector_db::CreateRequest_HNSWIndexConfig_DistanceMetric_L2) {
        distance_metric = HNSWIndex::DistanceMetric::L2;
    } else {
        distance_metric = HNSWIndex::DistanceMetric::Cosine;
    }
    replica_manager_->CreateTableOnReplica(name, vector_dimensionality, m, m0, ef_construction, ml, distance_metric, cache_size);
    return grpc::Status::OK;
}

grpc::Status ReplicaManagerServiceImpl::Drop(grpc::ServerContext* context, const vector_db::DropRequest* request, vector_db::DropResponse* response) {
    replica_manager_->DropTableOnReplica(request->table());
    return grpc::Status::OK;
}

} // namespace vector_db_engine
