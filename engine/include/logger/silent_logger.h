#ifndef VECTOR_DATABASE_SILENT_LOGGER_H
#define VECTOR_DATABASE_SILENT_LOGGER_H

#include <string>

#include "logger.h"

class SilentLogger : public Logger {
public:
    SilentLogger() = default;

    void Info(const std::string& message, const std::string& name) override;
    void Warn(const std::string& message, const std::string& name) override;
    void Error(const std::string& message, const std::string& name) override;
};

#endif //VECTOR_DATABASE_SILENT_LOGGER_H
