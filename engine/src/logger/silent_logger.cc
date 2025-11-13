#include "silent_logger.h"

#include <string>

namespace vector_db_engine {

void SilentLogger::Info(const std::string& message, const std::string& name) const {
    return;
}

void SilentLogger::Warn(const std::string& message, const std::string& name) const {
    return;
}

void SilentLogger::Error(const std::string& message, const std::string& name) const {
    return;
}

} // namespace vector_db_engine
