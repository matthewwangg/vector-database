#ifndef VECTOR_DATABASE_LOGGER_H
#define VECTOR_DATABASE_LOGGER_H

#include <string>

class Logger {
public:
    virtual ~Logger() = default;

    virtual void Info(const std::string& message);
    virtual void Warn(const std::string& message);
    virtual void Error(const std::string& message);

private:
    virtual std::string GetTimestamp();
};

#endif //VECTOR_DATABASE_LOGGER_H
