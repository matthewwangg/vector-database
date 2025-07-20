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
    grpc::Status BatchInsert(grpc::ServerContext* context, const vector_db::BatchInsertRequest* request, vector_db::BatchInsertResponse* response) override;
    grpc::Status BatchRemove(grpc::ServerContext* context, const vector_db::BatchRemoveRequest* request, vector_db::BatchRemoveResponse* response) override;
    grpc::Status BatchSearch(grpc::ServerContext* context, const vector_db::BatchSearchRequest* request, vector_db::BatchSearchResponse* response) override;
    grpc::Status Stats(grpc::ServerContext* context, const vector_db::StatsRequest* request, vector_db::StatsResponse* response) override;
    grpc::Status Metrics(grpc::ServerContext* context, const vector_db::MetricsRequest* request, vector_db::MetricsResponse* response) override;
    grpc::Status CreateTable(grpc::ServerContext* context, const vector_db::CreateTableRequest* request, vector_db::CreateTableResponse* response) override;
    grpc::Status DropTable(grpc::ServerContext* context, const vector_db::DropTableRequest* request, vector_db::DropTableResponse* response) override;
    grpc::Status HealthCheck(grpc::ServerContext* context, const vector_db::Empty* request, vector_db::Status* response) override;

    vector_db_engine::Engine* GetEngine() const;
private:
    std::unique_ptr<vector_db_engine::Engine> engine_;
};

#endif //VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H
