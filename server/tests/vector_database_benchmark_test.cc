#include "engine.h"
#include "vector_db_service_impl.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "server.grpc.pb.h"
#include "server.pb.h"

class VectorDatabaseBenchmarkTests : public ::testing::Test {
protected:
    void SetUp() override {
        std::string name = "unit_test_primary";
        std::string server_address = "localhost:50051";
        bool primary = true;
        float reindex_threshold = 0.25f;
        bool use_cache = false;
        std::string sync_server_address;
        std::vector<std::string> replicas = {"localhost:50053"};

        auto engine = std::make_unique<vector_db_engine::Engine>(name, primary, reindex_threshold, use_cache, sync_server_address, replicas, 60000, 90000);
        service_ = std::make_unique<VectorDatabaseServiceImpl>(std::move(engine));

        builder_.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder_.RegisterService(service_.get());

        server_ = builder_.BuildAndStart();
        stub_ = vector_db::VectorDatabase::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    }

    void TearDown() override {
        server_->Shutdown();
        server_->Wait();
        std::filesystem::remove_all(std::string(std::getenv("HOME")) + "/.vector_db/unit_test");
    }

    std::vector<float> MakeVector() {
        std::vector<float> vector;
        for (int i = 0; i < 384; ++i) {
            vector.push_back(static_cast<float>(rand()) / RAND_MAX);
        }
        return vector;
    }

    void MakeTableWithHNSWIndex() {
        grpc::ClientContext context;
        vector_db::CreateTableResponse response;
        vector_db::CreateTableRequest request;
        request.set_name("test_table");
        request.mutable_store_config()->set_vector_dimensionality(384);
        request.mutable_cache_config()->set_cache_size(32);
        auto* hnsw_config = request.mutable_hnsw_index_config();
        hnsw_config->set_m(16);
        hnsw_config->set_m0(32);
        hnsw_config->set_ef_construction(64);
        hnsw_config->set_ml(1.0f);
        hnsw_config->set_vector_dimensionality(384);
        hnsw_config->set_distance_metric(vector_db::CreateTableRequest_DistanceMetric_L2);

        grpc::Status status = stub_->CreateTable(&context, request, &response);
    }

    void MakeTableWithFlatIndex() {
        grpc::ClientContext context;
        vector_db::CreateTableResponse response;
        vector_db::CreateTableRequest request;

        request.set_name("test_table");
        request.mutable_store_config()->set_vector_dimensionality(384);
        request.mutable_cache_config()->set_cache_size(32);

        auto* flat_config = request.mutable_flat_index_config();
        flat_config->set_vector_dimensionality(384);
        flat_config->set_distance_metric(vector_db::CreateTableRequest_DistanceMetric_L2);

        grpc::Status create_table_status = stub_->CreateTable(&context, request, &response);
    }

    void Insert() {
        std::vector<float> vector = MakeVector();

        grpc::ClientContext context;
        vector_db::InsertResponse response;
        vector_db::InsertRequest request;
        request.set_id(1);
        request.set_content("test content");
        request.set_table("test_table");
        for (const auto& value : vector) {
            request.add_vector(value);
        }
        grpc::Status status = stub_->Insert(&context, request, &response);
    }

    void Remove() {
        grpc::ClientContext context;
        vector_db::RemoveResponse response;
        vector_db::RemoveRequest request;
        request.set_id(1);
        request.set_table("test_table");
    }

    void Search() {
        std::vector<float> vector = MakeVector();

        grpc::ClientContext context;
        vector_db::SearchResponse response;
        vector_db::SearchRequest request;
        request.set_table("test_table");
        request.set_k(1);
        request.set_search_parameter(64);
        for (const auto& value : vector) {
            request.add_query(value);
        }
        grpc::Status status = stub_->Search(&context, request, &response);
    }

    void BatchInsert() {
        std::vector<float> vector = MakeVector();

        grpc::ClientContext context;
        vector_db::BatchInsertResponse response;
        vector_db::BatchInsertRequest request;

        request.set_table("test_table");

        for (int i = 0; i < 100; ++i) {
            auto* vector_data = request.add_vector_data();
            vector_data->set_id(i);
            for (const auto& value: vector) {
                vector_data->add_vector(value);
            }
            vector_data->set_content("filler");
        }

        auto status = stub_->BatchInsert(&context, request, &response);
    }

    void BatchRemove() {
        grpc::ClientContext context;
        vector_db::BatchRemoveResponse response;
        vector_db::BatchRemoveRequest request;

        request.set_table("test_table");

        for (int i = 0; i < 100; ++i) {
            request.add_id(i);
        }

        auto status = stub_->BatchRemove(&context, request, &response);
    }

    void BatchSearch() {
        std::vector<float> vector = MakeVector();

        grpc::ClientContext context;
        vector_db::BatchSearchResponse response;
        vector_db::BatchSearchRequest request;

        request.set_table("test_table");

        for (int i = 0; i < 100; ++i) {
            auto* query = request.add_query();
            query->set_k(100);
            query->set_search_parameter(64);
            for (const auto& value : vector) {
                query->add_query(value);
            }
        }
    }

    std::unique_ptr<VectorDatabaseServiceImpl> service_;
    grpc::ServerBuilder builder_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<vector_db::VectorDatabase::Stub> stub_;
};

TEST_F(VectorDatabaseBenchmarkTests, HNSWInsert) {
    MakeTableWithHNSWIndex();
    auto start_time = std::chrono::steady_clock::now();
    Insert();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, HNSWRemove) {
    MakeTableWithHNSWIndex();
    auto start_time = std::chrono::steady_clock::now();
    Remove();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, HNSWSearch) {
    MakeTableWithHNSWIndex();
    Insert();
    auto start_time = std::chrono::steady_clock::now();
    Search();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, HNSWBatchInsert) {
    MakeTableWithHNSWIndex();
    auto start_time = std::chrono::steady_clock::now();
    BatchInsert();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;

    float average_time_in_milliseconds = total_time_in_milliseconds / 100;
    std::cout << "average time: " << average_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, HNSWBatchRemove) {
    MakeTableWithHNSWIndex();
    auto start_time = std::chrono::steady_clock::now();
    BatchRemove();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;

    float average_time_in_milliseconds = total_time_in_milliseconds / 100;
    std::cout << "average time: " << average_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, HNSWBatchSearch) {
    MakeTableWithHNSWIndex();
    auto start_time = std::chrono::steady_clock::now();
    BatchSearch();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;

    float average_time_in_milliseconds = total_time_in_milliseconds / 100;
    std::cout << "average time: " << average_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, FlatInsert) {
    MakeTableWithFlatIndex();
    auto start_time = std::chrono::steady_clock::now();
    Insert();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, FlatRemove) {
    MakeTableWithFlatIndex();
    auto start_time = std::chrono::steady_clock::now();
    Remove();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, FlatSearch) {
    MakeTableWithFlatIndex();
    Insert();
    auto start_time = std::chrono::steady_clock::now();
    Search();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, FlatBatchInsert) {
    MakeTableWithFlatIndex();
    auto start_time = std::chrono::steady_clock::now();
    BatchInsert();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;

    float average_time_in_milliseconds = total_time_in_milliseconds / 100;
    std::cout << "average time: " << average_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, FlatBatchRemove) {
    MakeTableWithFlatIndex();
    auto start_time = std::chrono::steady_clock::now();
    BatchRemove();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;

    float average_time_in_milliseconds = total_time_in_milliseconds / 100;
    std::cout << "average time: " << average_time_in_milliseconds << " ms" << std::endl;
}

TEST_F(VectorDatabaseBenchmarkTests, FlatBatchSearch) {
    MakeTableWithFlatIndex();
    auto start_time = std::chrono::steady_clock::now();
    BatchSearch();
    auto end_time = std::chrono::steady_clock::now();

    auto total_time_in_milliseconds = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()) / 1000;
    std::cout << "total time: " << total_time_in_milliseconds << " ms" << std::endl;

    float average_time_in_milliseconds = total_time_in_milliseconds / 100;
    std::cout << "average time: " << average_time_in_milliseconds << " ms" << std::endl;
}
