#include "vector_db_service_impl.h"

#include <memory>

#include "vector_store.h"
#include "hnsw_index.h"

VectorDatabaseServiceImpl::VectorDatabaseServiceImpl(std::unique_ptr<vector_db_engine::VectorStore> store)
    : store_(std::move(store))
{}

grpc::Status VectorDatabaseServiceImpl::Insert(grpc::ServerContext* context, const vector_db::InsertRequest* request, vector_db::InsertResponse* response) {
    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Remove(grpc::ServerContext* context, const vector_db::RemoveRequest* request, vector_db::RemoveResponse* response) {
    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Search(grpc::ServerContext* context, const vector_db::SearchRequest* request, vector_db::SearchResponse* response) {
    return grpc::Status::OK;
}