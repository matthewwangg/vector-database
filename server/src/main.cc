#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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

    if (argc < 3) {
        std::cout << "usage: " << argv[0] << " <name> <server-address> [flags]" << std::endl;
        return 1;
    }

    std::string name = argv[1];
    std::string server_address = argv[2];

    bool primary = true;
    float reindex_threshold = 0.25f;
    bool use_cache = true;
    int cleanup_interval = 60000;
    int sync_interval = 90000;
    std::string sync_server_address;
    std::vector<std::string> replicas;

    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--replica") {
            primary = false;
        } else if (arg == "--no-cache") {
            use_cache = false;
        } else if (arg == "--replica-address" && argc > i + 1) {
            replicas.emplace_back(argv[++i]);
        } else if  (arg == "--sync-server-address" && argc > i + 1) {
            sync_server_address = argv[++i];
        } else if (arg == "--cleanup-interval" && argc > i + 1) {
            cleanup_interval = std::stoi(argv[++i]);
        } else if (arg == "--sync-interval" && argc > i + 1) {
            sync_interval = std::stoi(argv[++i]);
        } else {
            std::cout << "usage: " << argv[0] << " <name> <server-address> [flags]" << std::endl;
            return 1;
        }
    }

    auto engine = std::make_unique<vector_db_engine::Engine>(name, primary, reindex_threshold, use_cache, sync_server_address, replicas, cleanup_interval, sync_interval);

    VectorDatabaseServiceImpl vector_db_service(std::move(engine));

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&vector_db_service);

    std::string type_indicator;
    if (primary) {
        type_indicator = " as primary";
    } else {
        type_indicator = " as replica";
    }
    vector_db_service.GetEngine()->GetLogger()->Info("server running on " + server_address + type_indicator, name);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    std::thread shutdown_thread([&server, &vector_db_service, &name]() {
        while (!shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownCheckInterval));
        }
        vector_db_service.GetEngine()->GetLogger()->Info("server shutting down...", name);
        server->Shutdown();
    });

    server->Wait();
    shutdown_thread.join();

    return 0;
}
