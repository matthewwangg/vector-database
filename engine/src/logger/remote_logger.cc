#include "remote_logger.h"

#include <chrono>
#include <iostream>

void RemoteLogger::Info(const std::string& message, const std::string& name) {
    SendLog("INFO", name, message);
}

void RemoteLogger::Warn(const std::string& message, const std::string& name) {
    SendLog("WARNING", name, message);
}

void RemoteLogger::Error(const std::string& message, const std::string& name) {
    SendLog("ERROR", name, message);
}

std::string RemoteLogger::GetTimestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buffer[10];
    std::strftime(buffer, sizeof(buffer), "%H:%M:%S", std::localtime(&t));
    return "[" + std::string(buffer) + "]";
}

void RemoteLogger::SendLog(const std::string& level, const std::string& message, const std::string source) {

}
