#ifndef VECTOR_DATABASE_LOCAL_LOGGER_H
#define VECTOR_DATABASE_LOCAL_LOGGER_H

#include <string>

#include "logger.h"

namespace vector_db_engine {

// LocalLogger is the logger with local output directly to stdout.
class LocalLogger : public Logger {
public:
    LocalLogger() = default;

    // Print INFO type message to stdout.
    void Info(const std::string& message, const std::string& name) override;

    // Print WARN type message to stdout.
    void Warn(const std::string& message, const std::string& name) override;

    // Print ERROR type message to stdout.
    void Error(const std::string& message, const std::string& name) override;

private:
    // Get the system timestamp.
    std::string GetTimestamp();
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_LOCAL_LOGGER_H
