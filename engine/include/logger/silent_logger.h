#ifndef VECTOR_DATABASE_SILENT_LOGGER_H
#define VECTOR_DATABASE_SILENT_LOGGER_H

#include <string>

#include "logger.h"

namespace vector_db_engine {

// SilentLogger is the logger with no output, which can be used for testing and quiet mode.
class SilentLogger : public Logger {
public:
    SilentLogger() = default;

    // No output.
    void Info(const std::string& message, const std::string& name) const override;

    // No output.
    void Warn(const std::string& message, const std::string& name) const override;

    // No output.
    void Error(const std::string& message, const std::string& name) const override;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_SILENT_LOGGER_H
