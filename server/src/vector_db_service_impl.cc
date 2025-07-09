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

grpc::Status VectorDatabaseServiceImpl::Stats(grpc::ServerContext* context, const vector_db::StatsRequest* request, vector_db::StatsResponse* response) {
    vector_db_engine::Engine::Stats stats = engine_->GetStats();

    response->set_vector_count(stats.vector_count);
    response->set_deleted_count(stats.deleted_count);
    response->set_stale_count(stats.stale_count);

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Metrics(grpc::ServerContext* context, const vector_db::MetricsRequest* request, vector_db::MetricsResponse* response) {
    vector_db_engine::Engine::Metrics metrics = engine_->GetMetrics();

    response->set_insert_count(metrics.insert_count);
    response->set_remove_count(metrics.remove_count);
    response->set_search_count(metrics.search_count);
    response->set_cleanup_count(metrics.cleanup_count);
    response->set_reindex_count(metrics.reindex_count);
    response->set_average_search_latency_ms(metrics.average_search_latency_ms);

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::HealthCheck(grpc::ServerContext* context, const vector_db::Empty* request, vector_db::Status* response) {
    response->set_successful(true);
    response->set_message("Healthy!");

    return grpc::Status::OK;
}
