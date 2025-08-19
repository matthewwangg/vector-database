#ifndef VECTOR_DATABASE_LOGGER_H
#define VECTOR_DATABASE_LOGGER_H

#include <string>

namespace vector_db_engine {

// Logger is the interface implemented by all logger types.
class Logger {
public:
    virtual ~Logger() = default;

    // Log an INFO type message.
    virtual void Info(const std::string& message, const std::string& name) = 0;

    // Log a WARN type message.
    virtual void Warn(const std::string& message, const std::string& name) = 0;

    // Log an ERROR type message.
    virtual void Error(const std::string& message, const std::string& name) = 0;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_LOGGER_H
