#include "common/utils.hpp"
#include "collect_layer/collector.hpp"
#include "cache_layer/info_cache_manager.hpp"
#include "logger/async_logger.hpp"
#include <csignal>
#include "collect_layer/trade_collector.hpp"
//单元测试
std::atomic<bool> run_loop(true);
void test_PeriodCtxConfig() {
    std::ostringstream oss;

    // 创建一个PeriodCtxConfig对象并初始化
    PeriodCtxConfig tradeCtx(CtxType::TRADE);
    tradeCtx.init();

    // 用oss构建输出字符串
    for (int i = 0; i < tradeCtx.unique_periods.size(); i++) {
        oss << "Period: " << tradeCtx.unique_periods[i] << " -> Functions: ";
        for (const auto& funcName : tradeCtx.per_period_feature_name_list[i]) {
            oss << funcName << ", ";
        }
        oss << "\n";
    }
    // 一次性输出测试结果
    std::cout << oss.str();
}

void test_ContractPeriodComputer(){
    auto logger_manager_sp =  std::make_shared<LoggerManager>();
    auto aggregator_manager_sp = std::make_shared<AggregatorManager>("parquet_result");
    auto contract_period_computer = std::make_shared<ContractPeriodComputer<TradeInfo,EXCHANGE::SH>>();
    contract_period_computer->init();
    contract_period_computer->register_logger(logger_manager_sp);
    contract_period_computer->bind_aggregator_manager(aggregator_manager_sp);
    contract_period_computer->init_per_period_ctx();
    aggregator_manager_sp->init("config","trade");

    //测试，推送一批数据
    //"000004.XSHE", "000005.XSHE", "000006.XSHE"
    auto today_start = [](){
        auto today = absl::CivilDay(2023,9,10);
        return absl::FromCivil(today,sh_tz.tz);
    }();
    auto past_time = absl::Hours(9) + absl::Minutes(30);
    auto sh_list = [](){
        std::vector<std::string> sh_list = {"000004.XSHE", "000005.XSHE", "000006.XSHE"};
        return vecConvertStrToArray<11>(sh_list);
    }();

    std::vector<PackedInfoSp<TradeInfo>> buffers_list;
    //打包N一份信息,3个ID各100条,其中买卖各50
    for(int k = 0; k < sh_list.size();k++){
        PackedInfoSp<TradeInfo> buffer_sp = std::make_shared<std::vector<std::shared_ptr<TradeInfo>>>();
        //测试，反复用同一份数据即可
        for(int i = 0; i < 100; i++){
            auto info_sp = std::make_shared<TradeInfo>();
            {
                info_sp->B_or_S = i%2;
                info_sp->SecurityID = sh_list[k];
                info_sp->TradPrice = k+1;
                info_sp->TradVolume = 10;
                info_sp->TradTime = today_start + past_time + absl::Milliseconds(i);
                info_sp->dur = past_time + absl::Milliseconds(i);
            }
            buffer_sp->push_back(info_sp);
        }
        buffers_list.push_back(buffer_sp);
    }
    //不利用时间封，此时时间不重要，能区分即可，仅测试
    int rec_count = 0;
    for(int i = 0; i < 30; i++){
        rec_count++;
        past_time += absl::Minutes(1);
        for(int k = 0; k < sh_list.size(); k++){
            contract_period_computer->process_minimum_period_info(buffers_list[k],k,rec_count);
        }
        int64_t tp = absl::ToUnixMicros(today_start + past_time);
        contract_period_computer->aggregator_notify(rec_count,tp);
    }
}

void signalHandler(int signum) {
    std::cout << "\nInterrupt signal (" << signum << ") received.\n";
    run_loop = false;
}

void test_CacheLayer(){
    auto logger_manager_sp = std::make_shared<LoggerManager>();
    if(!logger_manager_sp){
        std::cout << fmt::format("{} init failed","logger_manager") << std::endl;
    }
    //此时，aggregator_manager仅仅构造，但不实质启动，因为具体需要缓存表格，依赖于j
    auto aggregator_manager_sp = std::make_shared<AggregatorManager>("parquet_result");
    //先配置依赖最简单的TradeInfoCacheManager
    auto trade_info_cache_manager = std::make_shared<TradeInfoCacheManager>(logger_manager_sp);
    trade_info_cache_manager->run_deliver_threads(4);
    while(run_loop){
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "running...... \n";
    }
    std::cout << "finish \n";
    auto& buffer_sz_sp = trade_info_cache_manager->sz_trade_info_cache->security_id_to_buffer_map_sp->security_id_to_contract_buffer_map;
    auto& buffer_sh_sp = trade_info_cache_manager->sh_trade_info_cache->security_id_to_buffer_map_sp->security_id_to_contract_buffer_map;
    std::cout << trade_info_cache_manager->sz_trade_info_cache->today_start << std::endl;
    std::cout << trade_info_cache_manager->sz_trade_info_cache->info_queue_ptr->sizeGuess() << std::endl;
    for(auto & iter : buffer_sz_sp){
        for(char i : iter.first){
            std::cout << i;
        }
        std::cout << std::endl;
        std::cout << iter.second.buffer_ptr->sizeGuess() << std::endl;
    }
}

void test_CollectLayer(){
    auto logger_manager_sp = std::make_shared<LoggerManager>();
    logger_manager_sp->add_self_logger();
    auto aggregator_manager_sp = std::make_shared<AggregatorManager>("parquet_result");
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
        std::cout << "running...... \n";
    }
    std::cout << "finish \n";
}

void test_logger(){
    auto logger_manager_sp = std::make_shared<LoggerManager>();
    logger_manager_sp->add_self_logger();
    auto A_ptr = logger_manager_sp->get_logger("A");
    A_ptr->info("AAA");
    auto B_ptr = logger_manager_sp->get_logger("B");
    B_ptr->info("BBB");
}
int main()
{
    //signal(SIGINT, signalHandler);
    test_CollectLayer();
    return 0;
}