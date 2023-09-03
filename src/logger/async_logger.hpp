//
// Created by fanghz on 8/27/23.
//

#ifndef EMDS_ASYNC_LOGGER_HPP
#define EMDS_ASYNC_LOGGER_HPP

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/async_logger.h>
#include <spdlog/async.h>
#include <unordered_map>

class AsyncLoggerFactory{
public:
    AsyncLoggerFactory(){
        spdlog::init_thread_pool(8192,1);
    }
    std::shared_ptr<spdlog::async_logger> create(const std::string& name, const std::string& filename) {
        auto sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        auto logger = std::make_shared<spdlog::async_logger>(name, sink, spdlog::thread_pool(), spdlog::async_overflow_policy::block);
        spdlog::register_logger(logger);
        return logger;
    }
};

class LoggerManager {
    std::unordered_map<std::string, std::shared_ptr<spdlog::async_logger>> loggers;
    AsyncLoggerFactory factory;
public:
    std::shared_ptr<spdlog::async_logger> add_logger(const std::string& name, const std::string& filename) {
        loggers[name] = factory.create(name, filename);
        return loggers.at(name);
    }

    std::shared_ptr<spdlog::async_logger> get(const std::string& name) {
        return loggers.at(name);
    }

    void set_level(const std::string& name, spdlog::level::level_enum lvl) {
        loggers.at(name)->set_level(lvl);
    }
};

using LoggerPtr = std::shared_ptr<spdlog::async_logger>;

#endif //EMDS_ASYNC_LOGGER_HPP
