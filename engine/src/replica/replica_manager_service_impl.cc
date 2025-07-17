#include "replica_manager_service_impl.h"

#include <grpcpp/grpcpp.h>

#include "replica.grpc.pb.h"
#include "replica.pb.h"

namespace vector_db_engine {

ReplicaManagerServiceImpl::ReplicaManagerServiceImpl(Engine* engine)
    : engine_(engine)
{}

grpc::Status ReplicaManagerServiceImpl::Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) {
    for (const auto& entry : request->entry()) {
        engine_->ApplyWALEntry(entry);
    }
    return grpc::Status::OK;
}

grpc::Status ReplicaManagerServiceImpl::Drop(grpc::ServerContext* context, const vector_db::DropRequest* request, vector_db::DropResponse* response) {
    engine_->DropTableWithoutLock(request->table());
    return grpc::Status::OK;
}

} // namespace vector_db_engine
