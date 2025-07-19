#ifndef VECTOR_DATABASE_LOGGER_H
#define VECTOR_DATABASE_LOGGER_H

#include <string>

class Logger {
public:
    virtual ~Logger() = default;

    virtual void Info(const std::string& message, const std::string& name) = 0;
    virtual void Warn(const std::string& message, const std::string& name) = 0;
    virtual void Error(const std::string& message, const std::string& name) = 0;
};

#endif //VECTOR_DATABASE_LOGGER_H
