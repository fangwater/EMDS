#ifndef TRADE_CONTRACT_COLLECTOR_HPP
#define TRADE_CONTRACT_COLLECTOR_HPP
#include "period_signal_generator.hpp"

class TradeCollectorManager : public CollectorManager<TradeInfo>{
public:
    PeriodSignalGenerator<TradeInfo,EXCHANGE::SH> sh_sig_sender;
    PeriodSignalGenerator<TradeInfo,EXCHANGE::SZ> sz_sig_sender;
    std::jthread sh_jthread;
    std::jthread sz_jthread;
public:
    explicit TradeCollectorManager(const std::shared_ptr<AggregatorManager>& aggregator_sp):CollectorManager<TradeInfo>(aggregator_sp){};
    void init (const std::shared_ptr<LoggerManager>& logger_manager) override;
    void start_sig_sender(){
        sh_jthread = std::jthread(&PeriodSignalGenerator<TradeInfo,EXCHANGE::SH>::minimal_period_signal_generator, &sh_sig_sender);
        sz_jthread = std::jthread(&PeriodSignalGenerator<TradeInfo,EXCHANGE::SZ>::minimal_period_signal_generator, &sz_sig_sender);
    }
};

void TradeCollectorManager::init(const std::shared_ptr<LoggerManager> &logger_manager){
    CollectorManager<TradeInfo>::init(logger_manager);
    sh_sig_sender.register_logger(logger_manager);
    sz_sig_sender.register_logger(logger_manager);
    sh_sig_sender.register_collector(sh_collector);
    sz_sig_sender.register_collector(sz_collector);
    auto& computer = sz_period_computer;
    for(PeriodContext<TradeInfo>& period_ctx : sz_period_computer->per_period_ctx){
        //
        DLOG(INFO) << "Period "<<  period_ctx.period;
        for(auto& f_name : period_ctx.callback_names){
            DLOG(INFO) << f_name;
        }
    }

}
#endif