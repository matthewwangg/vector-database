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

class VectorDatabaseReplicaE2ETest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string name = "unit_test_primary";
        std::string server_address = "localhost:50051";
        bool primary = true;
        float reindex_threshold = 0.25f;
        bool use_cache = false;
        std::string sync_server_address;
        std::vector<std::string> replicas = {"localhost:50053"};

        auto primary_engine = std::make_unique<vector_db_engine::Engine>(name, primary, reindex_threshold, use_cache, sync_server_address, replicas, 5, 10);
        primary_service_ = std::make_unique<VectorDatabaseServiceImpl>(std::move(primary_engine));

        primary_builder_.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        primary_builder_.RegisterService(primary_service_.get());

        primary_server_ = primary_builder_.BuildAndStart();
        primary_stub_ = vector_db::VectorDatabase::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));

        name = "unit_test_replica";
        server_address = "localhost:50052";
        primary = false;
        reindex_threshold = 0.25f;
        use_cache = false;
        sync_server_address = "localhost:50053";
        replicas = {};

        auto replica_engine = std::make_unique<vector_db_engine::Engine>(name, primary, reindex_threshold, use_cache, sync_server_address, replicas, 5, 10);
        replica_service_ = std::make_unique<VectorDatabaseServiceImpl>(std::move(replica_engine));

        replica_builder_.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        replica_builder_.RegisterService(replica_service_.get());

        replica_server_ = replica_builder_.BuildAndStart();
        replica_stub_ = vector_db::VectorDatabase::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    }

    void TearDown() override {
        primary_server_->Shutdown();
        primary_server_->Wait();
        std::filesystem::remove_all(std::string(std::getenv("HOME")) + "/.vector_db/unit_test_primary");

        replica_server_->Shutdown();
        replica_server_->Wait();
        std::filesystem::remove_all(std::string(std::getenv("HOME")) + "/.vector_db/unit_test_replica");
    }

    std::vector<float> MakeVector() {
        std::vector<float> vector;
        for (int i = 0; i < 384; ++i) {
            vector.push_back(static_cast<float>(rand()) / RAND_MAX);
        }
        return vector;
    }

    std::unique_ptr<VectorDatabaseServiceImpl> primary_service_;
    grpc::ServerBuilder primary_builder_;
    std::unique_ptr<grpc::Server> primary_server_;
    std::unique_ptr<vector_db::VectorDatabase::Stub> primary_stub_;

    std::unique_ptr<VectorDatabaseServiceImpl> replica_service_;
    grpc::ServerBuilder replica_builder_;
    std::unique_ptr<grpc::Server> replica_server_;
    std::unique_ptr<vector_db::VectorDatabase::Stub> replica_stub_;
};

TEST_F(VectorDatabaseReplicaE2ETest, SyncCreate) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;

    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);

    auto* flat_config = create_table_request.mutable_flat_index_config();
    flat_config->set_vector_dimensionality(384);
    flat_config->set_distance_metric(vector_db::CreateTableRequest_DistanceMetric_L2);

    grpc::Status create_table_status = primary_stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    grpc::ClientContext list_tables_context;
    vector_db::ListTablesResponse list_tables_response;
    vector_db::ListTablesRequest list_tables_request;

    grpc::Status list_tables_status = replica_stub_->ListTables(&list_tables_context, list_tables_request, &list_tables_response);
    EXPECT_TRUE(list_tables_status.ok());
    ASSERT_EQ(list_tables_response.table_size(), 1);
    EXPECT_EQ(list_tables_response.table(0), "test_table");
}

TEST_F(VectorDatabaseReplicaE2ETest, SyncDrop) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;

    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);

    auto* flat_config = create_table_request.mutable_flat_index_config();
    flat_config->set_vector_dimensionality(384);
    flat_config->set_distance_metric(vector_db::CreateTableRequest_DistanceMetric_L2);

    grpc::Status create_table_status = primary_stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

    ASSERT_TRUE(create_table_status.ok());
    ASSERT_TRUE(create_table_response.successful());

    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    grpc::ClientContext list_tables_context;
    vector_db::ListTablesResponse list_tables_response;
    vector_db::ListTablesRequest list_tables_request;

    grpc::Status list_tables_status = replica_stub_->ListTables(&list_tables_context, list_tables_request, &list_tables_response);
    ASSERT_TRUE(list_tables_status.ok());
    ASSERT_EQ(list_tables_response.table_size(), 1);
    ASSERT_EQ(list_tables_response.table(0), "test_table");

    grpc::ClientContext drop_table_context;
    vector_db::DropTableResponse drop_table_response;
    vector_db::DropTableRequest drop_table_request;

    drop_table_request.set_name("test_table");

    grpc::Status drop_table_status = primary_stub_->DropTable(&drop_table_context, drop_table_request, &drop_table_response);

    ASSERT_TRUE(drop_table_status.ok());
    ASSERT_TRUE(drop_table_response.successful());

    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    grpc::ClientContext list_tables_context_2;
    vector_db::ListTablesResponse list_tables_response_2;
    vector_db::ListTablesRequest list_tables_request_2;

    grpc::Status list_tables_status_2 = replica_stub_->ListTables(&list_tables_context_2, list_tables_request_2, &list_tables_response_2);
    EXPECT_TRUE(list_tables_status_2.ok());
    EXPECT_EQ(list_tables_response_2.table_size(), 0);
}

TEST_F(VectorDatabaseReplicaE2ETest, SyncInsert) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;

    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);

    auto* flat_config = create_table_request.mutable_flat_index_config();
    flat_config->set_vector_dimensionality(384);
    flat_config->set_distance_metric(vector_db::CreateTableRequest_DistanceMetric_L2);

    grpc::Status create_table_status = primary_stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

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

    grpc::Status insert_status = primary_stub_->Insert(&insert_context, insert_request, &insert_response);

    ASSERT_TRUE(insert_status.ok());
    ASSERT_TRUE(insert_response.successful());

    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    grpc::ClientContext search_context;
    vector_db::SearchResponse search_response;
    vector_db::SearchRequest search_request;
    search_request.set_table("test_table");
    search_request.set_k(1);
    search_request.set_search_parameter(64);
    for (const auto& value : vector) {
        search_request.add_query(value);
    }

    grpc::Status search_status = replica_stub_->Search(&search_context, search_request, &search_response);

    EXPECT_TRUE(search_status.ok());
    ASSERT_EQ(search_response.data_size(), 1);
    EXPECT_EQ(search_response.data(0).id(), 1);
    EXPECT_EQ(search_response.data(0).content(), "test content");
}

TEST_F(VectorDatabaseReplicaE2ETest, SyncRemove) {
    grpc::ClientContext create_table_context;
    vector_db::CreateTableResponse create_table_response;
    vector_db::CreateTableRequest create_table_request;

    create_table_request.set_name("test_table");
    create_table_request.mutable_store_config()->set_vector_dimensionality(384);
    create_table_request.mutable_cache_config()->set_cache_size(32);

    auto* flat_config = create_table_request.mutable_flat_index_config();
    flat_config->set_vector_dimensionality(384);
    flat_config->set_distance_metric(vector_db::CreateTableRequest_DistanceMetric_L2);

    grpc::Status create_table_status = primary_stub_->CreateTable(&create_table_context, create_table_request, &create_table_response);

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

    grpc::Status insert_status = primary_stub_->Insert(&insert_context, insert_request, &insert_response);

    ASSERT_TRUE(insert_status.ok());
    ASSERT_TRUE(insert_response.successful());

    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    grpc::ClientContext search_context;
    vector_db::SearchResponse search_response;
    vector_db::SearchRequest search_request;
    search_request.set_table("test_table");
    search_request.set_k(1);
    search_request.set_search_parameter(64);
    for (const auto& value : vector) {
        search_request.add_query(value);
    }

    grpc::Status search_status = replica_stub_->Search(&search_context, search_request, &search_response);

    ASSERT_TRUE(search_status.ok());
    ASSERT_EQ(search_response.data_size(), 1);
    ASSERT_EQ(search_response.data(0).id(), 1);
    ASSERT_EQ(search_response.data(0).content(), "test content");

    grpc::ClientContext remove_context;
    vector_db::RemoveResponse remove_response;
    vector_db::RemoveRequest remove_request;
    remove_request.set_table("test_table");
    remove_request.set_id(1);

    grpc::Status remove_status = primary_stub_->Remove(&remove_context, remove_request, &remove_response);

    ASSERT_TRUE(remove_status.ok());
    ASSERT_TRUE(remove_response.successful());

    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    grpc::ClientContext search_context_2;
    vector_db::SearchResponse search_response_2;
    vector_db::SearchRequest search_request_2;
    search_request_2.set_table("test_table");
    search_request_2.set_k(1);
    search_request_2.set_search_parameter(64);
    for (const auto& value : vector) {
        search_request_2.add_query(value);
    }

    grpc::Status search_status_2 = replica_stub_->Search(&search_context_2, search_request_2, &search_response_2);

    EXPECT_TRUE(search_status_2.ok());
    EXPECT_EQ(search_response_2.data_size(), 0);
}


