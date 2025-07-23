#include "engine.h"
#include "vector_db_service_impl.h"

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
