#ifndef VECTOR_DATABASE_REMOTE_LOGGER_H
#define VECTOR_DATABASE_REMOTE_LOGGER_H

#include <memory>
#include <string>

#include "logger.h"

#include "log.grpc.pb.h"

class RemoteLogger : public Logger {
public:
    RemoteLogger(const std::string& logger_address);

    void Info(const std::string& message, const std::string& name) override;
    void Warn(const std::string& message, const std::string& name) override;
    void Error(const std::string& message, const std::string& name) override;

private:
    void SendLog(const std::string& level, const std::string& message, const std::string source);

    std::unique_ptr<logging::LogService::Stub> stub_;
};

#endif //VECTOR_DATABASE_REMOTE_LOGGER_H
