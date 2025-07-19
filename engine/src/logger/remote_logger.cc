#include "remote_logger.h"

#include <chrono>
#include <string>

#include <grpcpp/grpcpp.h>

#include "log.grpc.pb.h"
#include "log.pb.h"

RemoteLogger::RemoteLogger(const std::string& logger_address) {
    stub_ = logging::LogService::NewStub(grpc::CreateChannel(logger_address, grpc::InsecureChannelCredentials()));
}

void RemoteLogger::Info(const std::string& message, const std::string& name) {
    SendLog("INFO", name, message);
}

void RemoteLogger::Warn(const std::string& message, const std::string& name) {
    SendLog("WARNING", name, message);
}

void RemoteLogger::Error(const std::string& message, const std::string& name) {
    SendLog("ERROR", name, message);
}

void RemoteLogger::SendLog(const std::string& level, const std::string& message, const std::string source) {
    logging::LogEntry entry;
    entry.set_level(level);
    entry.set_message(message);
    entry.set_source(source);
    entry.set_hostname("127.0.0.1");

    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());

    auto* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(seconds.count());
    entry.set_allocated_timestamp(timestamp);

    grpc::ClientContext context;
    const char* api_key = std::getenv("LOG_SERVICE_API_KEY");
    context.AddMetadata("authorization", api_key);

    logging::LogResponse response;

    grpc::Status status = stub_->SendLog(&context, entry, &response);
}
