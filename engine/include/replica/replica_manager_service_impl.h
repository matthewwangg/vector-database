#ifndef VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H
#define VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H

#include <memory>

#include <grpcpp/grpcpp.h>

#include "engine.h"

#include "replica.grpc.pb.h"

class ReplicaManagerServiceImpl : public vector_db::ReplicaManager::Service {
public:
    explicit ReplicaManagerServiceImpl(std::unique_ptr<vector_db_engine::Engine>);

    grpc::Status Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) override;

private:
    std::unique_ptr<vector_db_engine::Engine> engine_;
};

#endif //VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H
