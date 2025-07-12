#include "vector_db_service_impl.h"

#include <memory>
#include <tuple>
#include <vector>

#include "engine.h"
#include "hnsw_index.h"

VectorDatabaseServiceImpl::VectorDatabaseServiceImpl(std::unique_ptr<vector_db_engine::Engine> engine)
    : engine_(std::move(engine))
{}

grpc::Status VectorDatabaseServiceImpl::Insert(grpc::ServerContext* context, const vector_db::InsertRequest* request, vector_db::InsertResponse* response) {
    vector_db_engine::Vector vector(request->vector().begin(), request->vector().end());

    bool successful = engine_->Insert(request->table(), request->id(), vector,request->content());
    response->set_successful(successful);

    if (!successful) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "insert failed (invalid input)");
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Remove(grpc::ServerContext* context, const vector_db::RemoveRequest* request, vector_db::RemoveResponse* response) {
    bool successful = engine_->Remove(request->table(), request->id());
    response->set_successful(successful);

    if (!successful) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "remove failed (invalid input)");
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Search(grpc::ServerContext* context, const vector_db::SearchRequest* request, vector_db::SearchResponse* response) {
    vector_db_engine::Vector query(request->query().begin(), request->query().end());

    std::vector<vector_db_engine::VectorStore::Data> results = engine_->Search(request->table(), query, request->k(), request->search_parameter());

    for (const auto& item : results) {
        auto* data = response->add_data();
        data->set_id(item.id);
        data->set_content(item.content);
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::BatchInsert(grpc::ServerContext* context, const vector_db::BatchInsertRequest* request, vector_db::BatchInsertResponse* response) {
    std::vector<std::tuple<vector_db_engine::Id, vector_db_engine::Vector, std::string>> vectors;

    for (const auto& vector_data : request->vector_data()) {
        vectors.emplace_back(vector_data.id(), vector_db_engine::Vector(vector_data.vector().begin(), vector_data.vector().end()), vector_data.content());
    }

    std::vector<bool> success_flags = engine_->BatchInsert(request->table(), vectors);
    for (bool successful : success_flags) {
        response->add_successful(successful);
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::BatchRemove(grpc::ServerContext* context, const vector_db::BatchRemoveRequest* request, vector_db::BatchRemoveResponse* response) {
    std::vector<vector_db_engine::Id> ids(request->id().begin(), request->id().end());

    std::vector<bool> success_flags = engine_->BatchRemove(request->table(), ids);
    for (bool successful : success_flags) {
        response->add_successful(successful);
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::BatchSearch(grpc::ServerContext* context, const vector_db::BatchSearchRequest* request, vector_db::BatchSearchResponse* response) {
    std::vector<std::tuple<vector_db_engine::Vector, std::size_t, std::size_t>> requests;
    for (const auto& query : request->query()) {
        requests.emplace_back(vector_db_engine::Vector(query.query().begin(), query.query().end()), query.k(), query.search_parameter());
    }

    std::vector<std::vector<vector_db_engine::VectorStore::Data>> results = engine_->BatchSearch(request->table(), requests);
    for (const auto& result_list : results) {
        auto* result = response->add_result();
        for (const auto& data : result_list) {
            auto* add_data = result->add_data();
            add_data->set_id(data.id);
            add_data->set_content(data.content);
        }
    }

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Stats(grpc::ServerContext* context, const vector_db::StatsRequest* request, vector_db::StatsResponse* response) {
    vector_db_engine::Engine::Stats stats = engine_->GetStats(request->table());

    response->set_vector_count(stats.vector_count);
    response->set_deleted_count(stats.deleted_count);
    response->set_stale_count(stats.stale_count);

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::Metrics(grpc::ServerContext* context, const vector_db::MetricsRequest* request, vector_db::MetricsResponse* response) {
    vector_db_engine::Engine::Metrics metrics = engine_->GetMetrics(request->table());

    response->set_insert_count(metrics.insert_count);
    response->set_remove_count(metrics.remove_count);
    response->set_search_count(metrics.search_count);
    response->set_cleanup_count(metrics.cleanup_count);
    response->set_reindex_count(metrics.reindex_count);
    response->set_average_search_latency_ms(metrics.average_search_latency_ms);

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::CreateTable(grpc::ServerContext* context, const vector_db::CreateTableRequest* request, vector_db::CreateTableResponse* response) {
    bool ok = engine_->CreateTable(request->name());
    response->set_successful(ok);

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::DropTable(grpc::ServerContext* context, const vector_db::DropTableRequest* request, vector_db::DropTableResponse* response) {
    bool ok = engine_->DropTable(request->name());
    response->set_successful(ok);

    return grpc::Status::OK;
}

grpc::Status VectorDatabaseServiceImpl::HealthCheck(grpc::ServerContext* context, const vector_db::Empty* request, vector_db::Status* response) {
    response->set_successful(true);
    response->set_message("Healthy!");

    return grpc::Status::OK;
}
