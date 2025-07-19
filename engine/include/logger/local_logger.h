#ifndef VECTOR_DATABASE_LOCAL_LOGGER_H
#define VECTOR_DATABASE_LOCAL_LOGGER_H

#include <string>

#include "logger.h"

class LocalLogger : public Logger {
public:
    void Info(const std::string& message);
    void Warn(const std::string& message);
    void Error(const std::string& message);

private:
    std::string GetTimestamp();
};

#endif //VECTOR_DATABASE_LOCAL_LOGGER_H
