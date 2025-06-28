#include "vector_db_service_impl.h"

#include <memory>

VectorDatabaseServiceImpl::VectorDatabaseServiceImpl(std::unique_ptr<vector_db_engine::VectorStore> store)
    : store_(store)
{}

grpc::Status VectorDatabaseServiceImpl::Insert(grpc::ServerContext* context, vector_db::InsertRequest* request, vector_db::InsertResponse* response) {

}

grpc::Status VectorDatabaseServiceImpl::Remove(grpc::ServerContext* context, vector_db::RemoveRequest* request, vector_db::RemoveResponse* response) {

}

grpc::Status VectorDatabaseServiceImpl::Search(grpc::ServerContext* context, vector_db::SearchRequest* request, vector_db::SearchResponse* response) {

}