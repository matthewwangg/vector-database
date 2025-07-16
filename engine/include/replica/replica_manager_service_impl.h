#ifndef VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H
#define VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H

#include <memory>

#include <grpcpp/grpcpp.h>

#include "engine.h"

#include "replica.grpc.pb.h"

namespace vector_db_engine {

class ReplicaManagerServiceImpl : public vector_db::ReplicaManager::Service {
public:
    explicit ReplicaManagerServiceImpl(Engine* engine);

    grpc::Status Sync(grpc::ServerContext* context, const vector_db::SyncRequest* request, vector_db::SyncResponse* response) override;

private:
    Engine* engine_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_SERVICE_IMPL_H
