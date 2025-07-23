#include "engine.h"
#include "vector_db_service_impl.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "server.grpc.pb.h"
#include "server.pb.h"

class VectorDatabaseE2ETest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string name = "unit_test";
        std::string server_address = "localhost:50051";
        bool primary = true;
        float reindex_threshold = 0.25f;
        bool use_cache = false;
        std::string sync_server_address;
        std::vector<std::string> replicas;

        auto engine = std::make_unique<vector_db_engine::Engine>(name, primary, reindex_threshold, use_cache, sync_server_address, replicas);
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

    std::unique_ptr<VectorDatabaseServiceImpl> service_;
    grpc::ServerBuilder builder_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<vector_db::VectorDatabase::Stub> stub_;
};

MATCHER_P2(MatchData, expected_id, expected_content, "") {
    return arg.id == expected_id && arg.content == expected_content;
}

TEST_F(VectorDatabaseE2ETest, HealthCheck) {
    grpc::ClientContext context;
    vector_db::Empty request;
    vector_db::Status response;
    grpc::Status status = stub_->HealthCheck(&context, request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.successful());
    EXPECT_EQ(response.message(), "Healthy!");
}

TEST_F(VectorDatabaseE2ETest, CreateTable) {
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
    hnsw_config->set_distance_metric(vector_db::CreateTableRequest_HNSWIndexConfig_DistanceMetric_L2);

    grpc::Status status = stub_->CreateTable(&context, request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.successful());
}

TEST_F(VectorDatabaseE2ETest, DropTable) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;
    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);
    auto* hnsw_config = create_table_request.mutable_hnsw_index_config();
    hnsw_config->set_m(16);
    hnsw_config->set_m0(32);
    hnsw_config->set_ef_construction(64);
    hnsw_config->set_ml(1.0f);
    hnsw_config->set_vector_dimensionality(384);
    hnsw_config->set_distance_metric(vector_db::CreateTableRequest_HNSWIndexConfig_DistanceMetric_L2);

    grpc::Status create_table_status = stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    grpc::ClientContext context;
    vector_db::DropTableResponse response;
    vector_db::DropTableRequest request;
    request.set_name("test_table");

    grpc::Status status = stub_->DropTable(&context, request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.successful());
}

TEST_F(VectorDatabaseE2ETest, ListTables) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;
    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);
    auto* hnsw_config = create_table_request.mutable_hnsw_index_config();
    hnsw_config->set_m(16);
    hnsw_config->set_m0(32);
    hnsw_config->set_ef_construction(64);
    hnsw_config->set_ml(1.0f);
    hnsw_config->set_vector_dimensionality(384);
    hnsw_config->set_distance_metric(vector_db::CreateTableRequest_HNSWIndexConfig_DistanceMetric_L2);

    grpc::Status create_table_status = stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    grpc::ClientContext context;
    vector_db::ListTablesResponse response;
    vector_db::ListTablesRequest request;

    grpc::Status status = stub_->ListTables(&context, request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_THAT(response.table(), ::testing::Contains("test_table"));
}

TEST_F(VectorDatabaseE2ETest, Insert) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;
    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);
    auto* hnsw_config = create_table_request.mutable_hnsw_index_config();
    hnsw_config->set_m(16);
    hnsw_config->set_m0(32);
    hnsw_config->set_ef_construction(64);
    hnsw_config->set_ml(1.0f);
    hnsw_config->set_vector_dimensionality(384);
    hnsw_config->set_distance_metric(vector_db::CreateTableRequest_HNSWIndexConfig_DistanceMetric_L2);

    grpc::Status create_table_status = stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    grpc::ClientContext context;
    vector_db::InsertResponse response;
    vector_db::InsertRequest request;
    request.set_id(1);
    request.set_content("test content");
    request.set_table("test_table");
    std::vector<float> vector = MakeVector();
    for (const auto& value : vector) {
        request.add_vector(value);
    }

    grpc::Status status = stub_->Insert(&context, request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.successful());
}

TEST_F(VectorDatabaseE2ETest, Remove) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;
    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);
    auto* hnsw_config = create_table_request.mutable_hnsw_index_config();
    hnsw_config->set_m(16);
    hnsw_config->set_m0(32);
    hnsw_config->set_ef_construction(64);
    hnsw_config->set_ml(1.0f);
    hnsw_config->set_vector_dimensionality(384);
    hnsw_config->set_distance_metric(vector_db::CreateTableRequest_HNSWIndexConfig_DistanceMetric_L2);

    grpc::Status create_table_status = stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    grpc::ClientContext insert_context;
    vector_db::InsertResponse insert_response;
    vector_db::InsertRequest insert_request;
    insert_request.set_id(1);
    insert_request.set_content("test content");
    insert_request.set_table("test_table");
    std::vector<float> vector = MakeVector();
    for (const auto& value : vector) {
        insert_request.add_vector(value);
    }

    grpc::Status insert_status = stub_->Insert(&insert_context, insert_request, &insert_response);

    EXPECT_TRUE(insert_status.ok());
    EXPECT_TRUE(insert_response.successful());

    grpc::ClientContext context;
    vector_db::RemoveResponse response;

    vector_db::RemoveRequest request;
    request.set_id(1);
    request.set_table("test_table");

    grpc::Status status = stub_->Remove(&context, request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.successful());
}

TEST_F(VectorDatabaseE2ETest, Search) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;

    vector_db::CreateTableRequest create_table_request;
    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);
    auto* hnsw_config = create_table_request.mutable_hnsw_index_config();
    hnsw_config->set_m(16);
    hnsw_config->set_m0(32);
    hnsw_config->set_ef_construction(64);
    hnsw_config->set_ml(1.0f);
    hnsw_config->set_vector_dimensionality(384);
    hnsw_config->set_distance_metric(vector_db::CreateTableRequest_HNSWIndexConfig_DistanceMetric_L2);

    grpc::Status create_table_status = stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    grpc::ClientContext insert_context;
    vector_db::InsertResponse insert_response;

    vector_db::InsertRequest insert_request;
    insert_request.set_id(1);
    insert_request.set_content("test content");
    insert_request.set_table("test_table");
    std::vector<float> vector = MakeVector();
    for (const auto& value : vector) {
        insert_request.add_vector(value);
    }

    grpc::Status insert_status = stub_->Insert(&insert_context, insert_request, &insert_response);

    EXPECT_TRUE(insert_status.ok());
    EXPECT_TRUE(insert_response.successful());

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

    EXPECT_TRUE(status.ok());
    ASSERT_EQ(response.data_size(), 1);
    EXPECT_EQ(response.data(0).id(), 1);
    EXPECT_EQ(response.data(0).content(), "test content");
}
