#ifndef VECTOR_DATABASE_LOCAL_LOGGER_H
#define VECTOR_DATABASE_LOCAL_LOGGER_H

#include <string>

#include "logger.h"

class LocalLogger : public Logger {
public:
    LocalLogger() = default;

    void Info(const std::string& message) override;
    void Warn(const std::string& message) override;
    void Error(const std::string& message) override;

protected:
    std::string GetTimestamp() override;
};

#endif //VECTOR_DATABASE_LOCAL_LOGGER_H
