#include <chrono>
#include <iostream>

#include "local_logger.h"

namespace vector_db_engine {

void LocalLogger::Info(const std::string& message, const std::string& name) const {
    std::cout << GetTimestamp() << " [INFO] [" << name << "] [127.0.0.1] " << message << std::endl;
}

void LocalLogger::Warn(const std::string& message, const std::string& name) const {
    std::cout << GetTimestamp() << " [WARNING] [" << name << "] [127.0.0.1] " << message << std::endl;
}

void LocalLogger::Error(const std::string& message, const std::string& name) const {
    std::cout << GetTimestamp() << " [ERROR] [" << name << "] [127.0.0.1] " << message << std::endl;
}

std::string LocalLogger::GetTimestamp() const {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buffer[10];
    std::strftime(buffer, sizeof(buffer), "%H:%M:%S", std::localtime(&t));
    return "[" + std::string(buffer) + "]";
}

} // namespace vector_db_engine


