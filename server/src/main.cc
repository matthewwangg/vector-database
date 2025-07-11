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

    float reindex_threshold = 0.25;

    auto engine = std::make_unique<vector_db_engine::Engine>(reindex_threshold);

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
