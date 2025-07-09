#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "vector_db_service_impl.h"
#include "vector_store.h"
#include "hnsw_index.h"

#include "server.grpc.pb.h"
#include "server.pb.h"

constexpr int kShutdownCheckInterval = 1000;

std::atomic<bool> shutdown = false;

void HandleSignals(int signal) {
    if (signal == SIGINT) {
        shutdown = true;
    }
    if (signal == SIGTERM) {
        shutdown = true;
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, HandleSignals);
    std::signal(SIGTERM, HandleSignals);

    std::string server_address = "0.0.0.0:50051";

    std::size_t m = 16;
    std::size_t m0 = 32;
    std::size_t ef_construction = 64;
    float ml = 1.0f;
    int vector_dimensionality = 384;
    auto distance_metric = vector_db_engine::HNSWIndex::DistanceMetric::L2;
    float reindex_threshold = 0.25;

    auto hnsw_index = std::make_unique<vector_db_engine::HNSWIndex>(m, m0, ef_construction, ml, distance_metric, vector_dimensionality);
    auto engine = std::make_unique<vector_db_engine::Engine>(std::move(hnsw_index), vector_dimensionality, reindex_threshold);

    VectorDatabaseServiceImpl vector_db_service(std::move(engine));

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&vector_db_service);

    std::cout << "server running on " << server_address << std::endl;
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    std::thread shutdown_thread([&server]() {
        while (!shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownCheckInterval));
        }
        std::cout << "server shutting down..." << std::endl;
        server->Shutdown();
    });

    server->Wait();
    shutdown_thread.join();

    return 0;
}
