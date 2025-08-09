#include "replica_manager_service_impl.h"

#include <grpcpp/grpcpp.h>

#include "replica.grpc.pb.h"
#include "replica.pb.h"

namespace vector_db_engine {

ReplicaManagerServiceImpl::ReplicaManagerServiceImpl(ReplicaManager* replica_manager)
    : replica_manager_(replica_manager)
{}

grpc::Status ReplicaManagerServiceImpl::Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) {
    std::size_t successful_count = 0;
    for (const auto& entry : request->entry()) {
        if (!replica_manager_->ApplyWALEntry(entry)) {
            break;
        }
        successful_count++;
    }
    response->set_successful_count(successful_count);
    return grpc::Status::OK;
}

} // namespace vector_db_engine
