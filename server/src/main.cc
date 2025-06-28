#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "vector_db_service_impl.h"
#include "vector_store.h"
#include "hnsw_index.h"

#include "vector_db.grpc.pb.h"
#include "vector_db.pb.h"

int main(int argc, char* argv[]) {
    std::string server_address = "0.0.0.0:50051";

    auto index = std::make_unique<vector_db_engine::HNSWIndex>(16, 32, 200, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 768);
    auto store = std::make_unique<vector_db_engine::VectorStore>(std::move(index), 768);

    VectorDatabaseServiceImpl vector_db_service(std::move(store));

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&vector_db_service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    server->Wait();
}
