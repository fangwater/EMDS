#ifndef TRADE_CONTRACT_COLLECTOR_HPP
#define TRADE_CONTRACT_COLLECTOR_HPP
#include "period_signal_generator.hpp"

class TradeCollectorManager : public CollectorManager<TradeInfo>{
public:
    PeriodSignalGenerator<TradeInfo,EXCHANGE::SH> sh_sig_sender;
    PeriodSignalGenerator<TradeInfo,EXCHANGE::SZ> sz_sig_sender;
public:
    explicit TradeCollectorManager(const std::shared_ptr<AggregatorManager>& aggregator_sp):CollectorManager<TradeInfo>(aggregator_sp){};
    void init(const std::shared_ptr<LoggerManager> &logger_manager);
};

void TradeCollectorManager::init(const std::shared_ptr<LoggerManager> &logger_manager) {
    sh_sig_sender.register_logger(logger_manager);
    sz_sig_sender.register_logger(logger_manager);

    sh_sig_sender.register_collector(sh_collector);
    sz_sig_sender.register_collector(sz_collector);
    CollectorManager<TradeInfo>::init(logger_manager);
}


#endif