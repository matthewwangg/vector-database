#ifndef VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H
#define VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H

#include <memory>

#include <grpcpp/grpcpp.h>

#include "engine.h"

#include "server.grpc.pb.h"

class VectorDatabaseServiceImpl : public vector_db::VectorDatabase::Service {
public:
    explicit VectorDatabaseServiceImpl(std::unique_ptr<vector_db_engine::Engine>);

    grpc::Status Insert(grpc::ServerContext* context, const vector_db::InsertRequest* request, vector_db::InsertResponse* response) override;
    grpc::Status Remove(grpc::ServerContext* context, const vector_db::RemoveRequest* request, vector_db::RemoveResponse* response) override;
    grpc::Status Search(grpc::ServerContext* context, const vector_db::SearchRequest* request, vector_db::SearchResponse* response) override;
    grpc::Status HealthCheck(grpc::ServerContext* context, const vector_db::Empty* request, vector_db::Status* response) override;
private:
    std::unique_ptr<vector_db_engine::Engine> engine_;
};

#endif //VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H
