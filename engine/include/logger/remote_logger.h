#ifndef VECTOR_DATABASE_REMOTE_LOGGER_H
#define VECTOR_DATABASE_REMOTE_LOGGER_H

#include <memory>
#include <string>

#include "logger.h"

#include "log.grpc.pb.h"

namespace vector_db_engine {

// RemoteLogger is the logger that sends logs directly to gRPC log aggregator.
class RemoteLogger : public Logger {
public:
    // Create the remote logger and initialize the log service stub.
    RemoteLogger(const std::string& logger_address);

    // Send INFO type log to log aggregator.
    void Info(const std::string& message, const std::string& name) const override;

    // Send WARN type log to log aggregator.
    void Warn(const std::string& message, const std::string& name) const override;

    // Send ERROR type log to log aggregator.
    void Error(const std::string& message, const std::string& name) const override;

private:
    // Send log to log aggregator.
    void SendLog(const std::string& level, const std::string& message, const std::string source) const;

    std::unique_ptr<logging::LogService::Stub> stub_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REMOTE_LOGGER_H
