#ifndef TRADE_CONTRACT_COLLECTOR_HPP
#define TRADE_CONTRACT_COLLECTOR_HPP
#include "period_signal_generator.hpp"

class TradeCollectorManager : public CollectorManager<TradeInfo>{
public:
    LoggerPtr logger;
public:
    explicit TradeCollectorManager(std::shared_ptr<AggregatorManager> aggregator_sp):
    CollectorManager<TradeInfo>(aggregator_sp){
        logger->info(fmt::format("Creating Trade Collector Manager at {}",get_current_time_as_string()));
    }
};

#endif