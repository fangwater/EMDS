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
#include <filesystem>
#include <fmt/format.h>

class AsyncLoggerFactory{
public:
    AsyncLoggerFactory() = delete;
    explicit AsyncLoggerFactory(int q_size){
        spdlog::init_thread_pool(q_size,1);
    }
    std::shared_ptr<spdlog::async_logger> create(const std::string& name, const std::string& filename) {
        auto sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        auto logger = std::make_shared<spdlog::async_logger>(name, sink, spdlog::thread_pool(), spdlog::async_overflow_policy::block);
        spdlog::register_logger(logger);
        return logger;
    }
};

using LoggerPtr = std::shared_ptr<spdlog::async_logger>;

class LoggerManager {
private:
    std::unordered_map<std::string, std::shared_ptr<spdlog::async_logger>> loggers;
    AsyncLoggerFactory factory;
    LoggerPtr self_logger;
    spdlog::level::level_enum default_level;
    // Helper method to ensure log directory exists
    static void ensure_log_directory_exists() {
        std::filesystem::path log_path("log");
        if (!std::filesystem::exists(log_path)) {
            std::filesystem::create_directory(log_path);
        }
    }
    std::shared_ptr<spdlog::async_logger> add_logger(const std::string& name) {
        ensure_log_directory_exists();
        std::string filename = fmt::format("log/{}.log",name);
        loggers[name] = factory.create(name, filename);
        set_level(name,default_level);
        return loggers.at(name);
    }
public:
    LoggerManager(): factory(8192){
        default_level = spdlog::level::level_enum::info;
    }
    void add_self_logger(){
        self_logger = add_logger("LoggerManager");
    }
    std::shared_ptr<spdlog::async_logger> get_logger(const std::string& name) {
        if (loggers.find(name) == loggers.end()) {
            // Logger not found, add it
            self_logger->info(fmt::format("Add new logger {}",name));
            return add_logger(name);
        }
        return loggers.at(name);
    }
    void set_level(const std::string& name, spdlog::level::level_enum lvl) {
        loggers.at(name)->set_level(lvl);
    }
};



#endif //EMDS_ASYNC_LOGGER_HPP
