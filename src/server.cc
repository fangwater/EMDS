#include "common/utils.hpp"
#include "collect_layer/collector.hpp"
#include "cache_layer/info_cache_manager.hpp"
#include "logger/async_logger.hpp"
#include "collect_layer/trade_collector.hpp"
#include <csignal>
#include <cstdlib>

std::atomic<bool> run_loop(true);
void EMDS_FINISH() {
    LOG(INFO) << "ALL log flushed, Exit EMDS server!";
}
void signalHandler(int signum) {
    LOG(INFO) << fmt::format("EMDS server receive interrupt signal at {}",get_current_time_as_string());
    run_loop = false;
}


class ConfigMaker{
public:
    static void prepare();
};


void ConfigMaker::prepare(){
    LOG(INFO) << fmt::format("Processing hfq_multi.parquet, loading contract list and closeprice ......");
    if(!std::filesystem::exists("config")){
        throw std::runtime_error("config folder not find");
    }
    json config = open_json_file("config/system.json");
    //嵌入python脚本，获取当前的天数, 需要track的股票数，和前一天的收盘价closeprice
    std::string hfq_table_path = config["system"]["hfq_multi_parquet_path"];
    std::string today_str = config["system"]["date"];
    std::system(fmt::format("python3 script.py {} {}", hfq_table_path, today_str).c_str());
    LOG(INFO) << fmt::format("Success load info from hfq_multi.parquet.");
}

int main()
 {
    signal(SIGINT, signalHandler);
    std::at_quick_exit(EMDS_FINISH);
    ConfigMaker::prepare();
    auto logger_manager_sp = std::make_shared<LoggerManager>();
    logger_manager_sp->add_self_logger();
    auto aggregator_manager_sp = [](){
        json config = open_json_file("config/system.json");
        if(config["mode"]["dump"] == true){
            std::string path = config["mode"]["dump_dir"];
            LOG(INFO) << fmt::format("Using publish + dump mode, path: {}",path);
            return std::make_shared<AggregatorManager>("parquet_result");
        }else{
            LOG(INFO) << fmt::format("Using publish only mode");
            return std::make_shared<AggregatorManager>();
        }
    }();
    auto trade_info_cache_manager = std::make_shared<TradeInfoCacheManager>(logger_manager_sp);
    trade_info_cache_manager->run_deliver_threads(4);

    auto trade_collector_sp = std::make_shared<TradeCollectorManager>(aggregator_manager_sp);
    trade_collector_sp->register_collect_target_buffer_map(
            trade_info_cache_manager->sh_trade_info_cache->security_id_to_buffer_map_sp,
            trade_info_cache_manager->sz_trade_info_cache->security_id_to_buffer_map_sp
    );

    trade_collector_sp->init(logger_manager_sp);
    trade_collector_sp->start_sig_sender();
    aggregator_manager_sp->init("config","trade");
    while(run_loop){
        std::this_thread::sleep_for(std::chrono::seconds(1));
        fmt::print("EMDS running...... Current time: {}\n", get_current_time_as_string());
    }
    aggregator_manager_sp.reset();
    logger_manager_sp.reset();
    std::quick_exit(0);
    return 0;
 }
