#ifndef CONTRACT_COLLECTOR_MANAGER_HPP
#define CONTRACT_COLLECTOR_MANAGER_HPP
#include <memory>
#include "../common/info.hpp"
#include <thread>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include "../common/time_utils.hpp"
#include "../common/fmt_expand.hpp"
#include "../common/utils.hpp"
#include "../common/config_type.hpp"
#include "collector.hpp"
#include "../common/info.hpp"
#include "contract_buffer.hpp"
#include "../aggregate_layer/aggregator_manager.hpp"

template<EXCHANGE EX>
using TradeCollector = ContractBufferMapCollector<TradeInfo,EX>;

template<typename T>
class CollectorManager{
public:
    LoggerPtr logger;
    //buffer_map
    std::shared_ptr<ContractBufferMap<T,EXCHANGE::SH>> sh_buffer_map;
    std::shared_ptr<ContractBufferMap<T,EXCHANGE::SH>> sz_buffer_map;
    //collector
    std::shared_ptr<ContractBufferMapCollector<T,EXCHANGE::SH>> sh_collector;
    std::shared_ptr<ContractBufferMapCollector<T,EXCHANGE::SZ>> sz_collector;
    //period_computer
    std::shared_ptr<ContractPeriodComputer<T,EXCHANGE::SH>> sh_period_computer;
    std::shared_ptr<ContractPeriodComputer<T,EXCHANGE::SZ>> sz_period_computer;
    //aggregator_manager
    std::shared_ptr<AggregatorManager> aggregator_manager;
public:
    explicit CollectorManager(const std::shared_ptr<AggregatorManager>& aggregator_sp){
        if(aggregator_sp){
            aggregator_manager = aggregator_sp;
        }else{
            throw std::runtime_error("aggregator_manager is uninitialized");
        }
    }
    void init(const std::shared_ptr<LoggerManager> &logger_manager){
        //CollectorManager 需要输出日志
        logger = logger_manager->get_logger("CollectorManager[{}]", str_type_info<T>());
        logger->info(fmt::format("Create CollectorManager[{}] at {}", str_type_info<T>(),get_current_time_as_string()));

        //buffer不需要日志, 其职责仅为接收来自缓存的插入
        sh_buffer_map = std::make_shared<ContractBufferMap<T,EXCHANGE::SH>>();
        sh_buffer_map->init();
        sz_buffer_map = std::make_shared<ContractBufferMap<T,EXCHANGE::SZ>>();
        sz_buffer_map->init();

        sh_collector = std::make_shared<ContractBufferMapCollector<T,EXCHANGE::SH>>();
        sh_collector->init();
        sz_collector = std::make_shared<ContractBufferMapCollector<T,EXCHANGE::SZ>>();
        sz_collector->init();

        //计算模块会输出日志,包含一些函数j
        //
        sh_period_computer = std::make_shared<ContractPeriodComputer<T,EXCHANGE::SH>>();
        sh_period_computer->register_logger(logger_manager);
        sh_period_computer->init();
        sh_period_computer->init_per_period_ctx();

        sz_period_computer = std::make_shared<ContractPeriodComputer<T,EXCHANGE::SZ>>();
        sz_period_computer->register_logger(logger_manager);
        sz_period_computer->init();
        sh_period_computer->init_per_period_ctx();

        sh_collector->bind_contract_period_computer(sh_period_computer);
        sh_collector->bind_contract_buffer_map(sh_buffer_map);
        sz_collector->bind_contract_period_computer(sz_period_computer);
        sz_collector->bind_contract_buffer_map(sz_buffer_map);
        sh_period_computer->bind_aggregator_manager(aggregator_manager);
        sz_period_computer->bind_aggregator_manager(aggregator_manager);

    };
};

#endif