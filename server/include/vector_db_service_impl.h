#ifndef VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H
#define VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H

#include <memory>

#include <grpcpp/grpcpp.h>

#include "engine.h"

#include "server.grpc.pb.h"

// VectorDatabaseServiceImpl is the implementation of the gRPC VectorDatabase service. It achieves all the core vector database operations using the engine it owns.
class VectorDatabaseServiceImpl : public vector_db::VectorDatabase::Service {
public:
    // Creates the service implementation with an engine to handle the core operations.
    explicit VectorDatabaseServiceImpl(std::unique_ptr<vector_db_engine::Engine>);

    // Inserts a single vector into the specified table.
    grpc::Status Insert(grpc::ServerContext* context, const vector_db::InsertRequest* request, vector_db::InsertResponse* response) override;

    // Removes a single vector by ID from the specified table.
    grpc::Status Remove(grpc::ServerContext* context, const vector_db::RemoveRequest* request, vector_db::RemoveResponse* response) override;

    // Search the specified table for k-nearest neighbors (or approximate nearest neighbors) to the query vector.
    grpc::Status Search(grpc::ServerContext* context, const vector_db::SearchRequest* request, vector_db::SearchResponse* response) override;

    // Inserts a batch of vectors into the specified table.
    grpc::Status BatchInsert(grpc::ServerContext* context, const vector_db::BatchInsertRequest* request, vector_db::BatchInsertResponse* response) override;

    // Removes a batch of vectors by ID from the specified table.
    grpc::Status BatchRemove(grpc::ServerContext* context, const vector_db::BatchRemoveRequest* request, vector_db::BatchRemoveResponse* response) override;

    // Searches the specified table for k-nearest neighbors (or approximate nearest neighbors) to all the query vectors.
    grpc::Status BatchSearch(grpc::ServerContext* context, const vector_db::BatchSearchRequest* request, vector_db::BatchSearchResponse* response) override;

    // Gets the statistics for the specified table.
    grpc::Status Stats(grpc::ServerContext* context, const vector_db::StatsRequest* request, vector_db::StatsResponse* response) override;

    // Gets the metrics for the specified table.
    grpc::Status Metrics(grpc::ServerContext* context, const vector_db::MetricsRequest* request, vector_db::MetricsResponse* response) override;

    // Creates a database table with the requested specifications.
    grpc::Status CreateTable(grpc::ServerContext* context, const vector_db::CreateTableRequest* request, vector_db::CreateTableResponse* response) override;

    // Drops the specified table from the database and clears all its data.
    grpc::Status DropTable(grpc::ServerContext* context, const vector_db::DropTableRequest* request, vector_db::DropTableResponse* response) override;

    // List all currently existing tables on the database.
    grpc::Status ListTables(grpc::ServerContext* context, const vector_db::ListTablesRequest* request, vector_db::ListTablesResponse* response) override;

    // Returns a successful status and serves as a health check for the service.
    grpc::Status HealthCheck(grpc::ServerContext* context, const vector_db::Empty* request, vector_db::Status* response) override;

    vector_db_engine::Engine* GetEngine() const { return engine_.get(); };
private:
    std::unique_ptr<vector_db_engine::Engine> engine_;
};

#endif //VECTOR_DATABASE_VECTOR_DB_SERVICE_IMPL_H
