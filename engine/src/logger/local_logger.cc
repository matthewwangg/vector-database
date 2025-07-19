#include <iostream>

#include "local_logger.h"

void LocalLogger::Info(const std::string& message) {
    std::cout << "[INFO] [vector_database] [127.0.0.1] " << message << std::endl;
}

void LocalLogger::Warn(const std::string& message) {
    std::cout << "[WARNING] [vector_database] [127.0.0.1] " << message << std::endl;
}

void LocalLogger::Error(const std::string& message) {
    std::cout << "[ERROR] [vector_database] [127.0.0.1] " message << std::endl;
}

std::string LocalLogger::GetTimestamp() {
    return "";
}
