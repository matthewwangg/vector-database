#ifndef VECTOR_DATABASE_REMOTE_LOGGER_H
#define VECTOR_DATABASE_REMOTE_LOGGER_H

#include <string>

#include "logger.h"

class RemoteLogger : public Logger {
public:
    RemoteLogger(const std::string& logger_address);

    void Info(const std::string& message, const std::string& name) override;
    void Warn(const std::string& message, const std::string& name) override;
    void Error(const std::string& message, const std::string& name) override;

protected:
    std::string GetTimestamp() override;

private:
    void SendLog(const std::string& level, const std::string& message, const std::string source);
};

#endif //VECTOR_DATABASE_REMOTE_LOGGER_H
