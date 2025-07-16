#include "replica_manager_service_impl.cc"

grpc::Status ReplicaManagerServiceImpl::Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) {
    for (const auto& entry : request->entry) {
        engine_->ApplyWALEntry(entry);
    }
    return grpc::Status::OK;
}
