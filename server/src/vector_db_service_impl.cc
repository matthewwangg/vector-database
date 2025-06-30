#include "vector_db_service_impl.h"

#include <memory>

#include "engine.h"
#include "hnsw_index.h"

VectorDatabaseServiceImpl::VectorDatabaseServiceImpl(std::unique_ptr<vector_db_engine::Engine> engine)
    : engine_(std::move(engine))
{}

grpc::Status VectorDatabaseServiceImpl::Insert(grpc::ServerContext* context, const vector_db::InsertRequest* request, vector_db::InsertResponse* response) {
    vector_db_engine::Vector vector(request->vector().begin(), request->vector().end());

    bool successful = engine_->Insert(request->id(), vector,request->content());
    response->set_successful(successful);

    if (!successful) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "insert failed (invalid input)");
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Remove(grpc::ServerContext* context, const vector_db::RemoveRequest* request, vector_db::RemoveResponse* response) {
    bool successful = engine_->Remove(request->id());
    response->set_successful(successful);

    if (!successful) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "remove failed (invalid input)");
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Search(grpc::ServerContext* context, const vector_db::SearchRequest* request, vector_db::SearchResponse* response) {
    vector_db_engine::Vector query(request->query().begin(), request->query().end());

    std::vector<vector_db_engine::VectorStore::Data> data = engine_->Search(query, request->k(), request->search_parameter());

    for (const auto& item : data) {
        auto* result = response->add_data();
        result->set_content(item.content);
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::HealthCheck(grpc::ServerContext* context, const vector_db::Empty* request, vector_db::Status* response) {
    response->set_successful(true);
    response->set_message("Healthy!");

    return grpc::Status::OK;
}

