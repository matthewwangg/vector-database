#ifndef VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H
#define VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H

#include <memory>

#include <grpcpp/grpcpp.h>

#include "engine.h"

#include "replica.grpc.pb.h"

namespace vector_db_engine {

// ReplicaManagerServiceImpl is the implementation of the gRPC ReplicaManager service.
class ReplicaManagerServiceImpl : public vector_db::ReplicaManager::Service {
public:
    // Create the replica manager service implementation and keep the replica manager as a member but don't take ownership.
    explicit ReplicaManagerServiceImpl(ReplicaManager* replica_manager);

    // Trigger the sync operation on the replica by each repeated WAL entry and returns the number of successful applications.
    grpc::Status Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) override;

    // Returns the checksum of the local vector store for checksum validation.
    grpc::Status GetChecksum(grpc::ServerContext* context, const vector_db::GetChecksumRequest* request, vector_db::GetChecksumResponse* response) override;

private:
    ReplicaManager* replica_manager_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H
