#ifndef CONTRACT_COLLECTOR_HPP
#define CONTRACT_COLLECTOR_HPP
#include <memory>
#include <thread>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include <tbb/tbb.h>
#include <tbb/task_arena.h>
#include "../cache_layer/contract_buffer.hpp"
#include "../common/info.hpp"
#include "../common/time_utils.hpp"
#include "../common/fmt_expand.hpp"
#include "../common/utils.hpp"
#include "../common/config_type.hpp"
#include "../aggregate_layer/period_result.hpp"
#include "../common/callback_factory.hpp"
#include "../aggregate_layer/period_result.hpp"
#include "../aggregate_layer/aggregator_manager.hpp"
#include "../logger/async_logger.hpp"

template<typename T>
class PeriodContext{
public:
    int period;
    /*each contract has itself accumulated_info for target unique period, manager here*/
    std::vector<PackedInfoSp<T>> accumulated_infos;
    std::vector<CallBackObjPtr<T>> callbacks;
    std::vector<std::string> callback_names;
    explicit PeriodContext(int period_of_context):period(period_of_context){};
    std::size_t append_infos(PackedInfoSp<T> src, int index){
        auto& accumulated_info = accumulated_infos[index];
        if(!src->size()){
            return accumulated_info->size();
        }else{
            accumulated_info->reserve(accumulated_info->size() + src->size());
            std::copy(src->begin(), src->end(), std::back_inserter(*accumulated_info));
        }
        return accumulated_info->size();
    }
};

template<typename T, EXCHANGE EX>
class ContractPeriodComputer{
public:
    std::vector<std::array<char,11>> security_ids;
    LoggerPtr logger;
    std::vector<PeriodContext<T>> per_period_ctx;
    std::shared_ptr<AggregatorManager> aggregator;
private:
    void update_trade_period_call_config();
    //TODO
    //void update_depth_period_call_config();
    //void update_order_period_call_config();
public:
    ContractPeriodComputer() = default;
    void register_logger(const std::shared_ptr<LoggerManager>& logger_manager){
        logger = logger_manager->get_logger(fmt::format("ContractPeriodComputer_{}",str_type_ex<T,EX>()));
    }
    //TODO: 需要重新考虑盘中重启的情况
    /**
     *
     * @param minimum_period_info 数据buffer指针
     * @param i 在股票序列中的顺序
     * @param rec_count 第几次发送
     */
    void process_minimum_period_info(PackedInfoSp<T> minimum_period_info, int32_t i, int64_t rec_count);
    void init_per_period_ctx();
    void init();
    void bind_aggregator_manager(const std::shared_ptr<AggregatorManager>& manager_sp);
    void aggregator_notify(int rec_count,int64_t tp){
        for(PeriodContext<T>& period_ctx : per_period_ctx){
            if(!(rec_count % period_ctx.period)){
                logger->info(fmt::format("Record for period {}",period_ctx.period));
                aggregator->period_finish(period_ctx.period,tp);
            }
        }
    }
};

template<typename T, EXCHANGE EX>
void ContractPeriodComputer<T,EX>::update_trade_period_call_config() {
    std::ifstream inputFile("config/period_call.json");
    if (!inputFile.is_open()) {
        throw std::runtime_error("Failed to open the config file.");
    }
    json config;
    inputFile >> config;
    inputFile.close();
    if (config.find("trade") == config.end()) {
        throw std::runtime_error("The 'trade' field is not found in the config file.");
    }
    config["ip"] = get_host_ip();
    //清空旧的数据，重新加入注册
    config["trade"] = json::array();
    //发布源的最低端口号，递增使用
    int port_begin = [](){
        SystemParam system_param;
        system_param.init();
        return system_param.port_begin;
    }();
    for(int i = 0; i < per_period_ctx.size(); i++){
        auto& period_context = per_period_ctx.at(i);
        json entry = json::object();
        entry["period"] = period_context.period;
        entry["called_func"] = period_context.callback_names;
        int need_port_count = period_context.callback_names.size();
        std::vector<int> ports_of_period(need_port_count);
        std::iota(ports_of_period.begin(), ports_of_period.end(), port_begin);
        port_begin += need_port_count;
        entry["port"] = ports_of_period;
        config["trade"].push_back(entry);
    }
    std::ofstream outputFile("config/period_call.json");
    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed to open the config file for writing.");
    }
    outputFile << config.dump(4);  // '4' is for indentation
    outputFile.close();
}

template<typename T, EXCHANGE EX>
void ContractPeriodComputer<T, EX>::init(){
    SecurityId security_id_config;
    security_id_config.init();
    if constexpr (EX == EXCHANGE::SH){
        security_ids = security_id_config.sh;
    }else if constexpr (EX == EXCHANGE::SZ){
        security_ids = security_id_config.sz;
    }else{
        throw std::runtime_error("Invalid exchange type");
    }
}

template<typename T, EXCHANGE EX>
void ContractPeriodComputer<T,EX>::bind_aggregator_manager(const std::shared_ptr<AggregatorManager>& manager_sp) {
    if(manager_sp){
        aggregator = manager_sp;
    }else{
        throw std::runtime_error("bind uninitialized aggregator manager to contract_period_computer");
    }
}

template<typename T, EXCHANGE EX>
void ContractPeriodComputer<T,EX>::init_per_period_ctx() {
    if constexpr (std::is_same_v<T, TradeInfo>){
        PeriodCtxConfig trade_ctx_config(CtxType::TRADE);
        int contract_size = [](){
            ClosePrice close_price;
            close_price.init();
            if constexpr (EX == EXCHANGE::SH){
                return close_price.sh.size();
            }else if constexpr (EX == EXCHANGE::SZ){
                return close_price.sz.size();
            }else{
                throw std::runtime_error("Unexpected EXCHANGE Type");
            }
        }();
        trade_ctx_config.init();
        for(int i = 0; i < trade_ctx_config.unique_periods.size(); i++){
            /*需要知道合约个数*/
            PeriodContext<TradeInfo> period_ctx(trade_ctx_config.unique_periods.at(i));
            period_ctx.callback_names = trade_ctx_config.per_period_feature_name_list.at(i);
            for(auto& cb_name : period_ctx.callback_names){
                CallBackObjFactory factory;
                auto callback_obj_ptr = factory.create_trade_function_callback<EX>(cb_name);
                period_ctx.callbacks.push_back(std::move(callback_obj_ptr));
            }
            for(int i = 0; i < contract_size; i++){
                auto ptr = std::make_shared<std::vector<std::shared_ptr<TradeInfo>>>();
                period_ctx.accumulated_infos.push_back(std::move(ptr));
            }
            per_period_ctx.push_back(std::move(period_ctx));
        }
        update_trade_period_call_config();
    }
//    else if constexpr (std::is_same_v<T,OrderInfo>){
//        //TODO
//    }else if constexpr (std::is_same_v<T,DepthInfo>){
//        //TODO
//    }
    else{
        throw std::runtime_error("Unexpected Info Type");
    }
}

template<typename T, EXCHANGE EX>
void ContractPeriodComputer<T,EX>::process_minimum_period_info(PackedInfoSp<T> minimum_period_info, int i, const int64_t rec_count) {
    for(PeriodContext<T>& period_ctx : per_period_ctx){
        period_ctx.append_infos(minimum_period_info,i);
        if(!(rec_count % period_ctx.period)){
            //收集此周期所注册feature函数的计算结果
            std::vector<std::vector<double>> callback_results;
            //顺次调用注册的callback函数，完成当前period所需的计算
            for(int j = 0; j < period_ctx.callbacks.size(); j++){
                auto res = period_ctx.callbacks.at(j)->calculate(minimum_period_info,i);
                callback_results.push_back(std::move(res));
            }
            PeriodResult period_res{
                    .period = period_ctx.period,
                    .security_id = security_ids[i],
                    .results = std::move(callback_results)
            };
            //
            // if(period_res.security_id == std::array<char, 11>{'0', '0', '0', '0', '1', '8', '.', 'X', 'S', 'H', 'E'}){
            //     printf("Get 000018.XSHE\n");
            //     printf("%d\n",minimum_period_info->size());
            //     if (std::isnan(period_res.results[0][0])) {
            //         std::cout << "feature is NaN" << std::endl;}
            // }
            aggregator->commit(std::move(period_res));
            //释放当前period的数据
            period_ctx.accumulated_infos[i]->clear();
        }
    }
}

template<typename T, EXCHANGE EX>
class ContractBufferMapCollector{
public:
    std::shared_ptr<ContractBufferMap<T,EX>> contract_buffer_map;
    std::shared_ptr<ContractPeriodComputer<T,EX>> contract_period_computer;
    std::vector<std::array<char,11>> security_ids;
    mutable std::mutex mtx;
    int64_t latency;
    int64_t min_period;
    int32_t active_threads;
    absl::Time last_update_time;
    absl::CivilDay today;
    int64_t rec_count;
    std::unique_ptr<tbb::task_arena> node_arena;
    int scale_rate;
public:
    ContractBufferMapCollector();
public:
    int signal_handler(absl::Time tp);
    int special_signal_handler(absl::Time tp, int64_t sleep_time); 
    void init();
    int bind_contract_buffer_map(std::shared_ptr<ContractBufferMap<T,EX>> _contract_buffer_map);
    int bind_contract_period_computer(std::shared_ptr<ContractPeriodComputer<T,EX>> _contract_period_computer);
public:
    PackedInfoSp<T> collect_info_by_id(std::array<char,11>& security_id, absl::Time last_tp);
    int run_collect(absl::Time t);
};
template<typename T, EXCHANGE EX>
ContractBufferMapCollector<T,EX>::ContractBufferMapCollector():
    rec_count(0),latency(0),active_threads(0){}

template<typename T, EXCHANGE EX>
int ContractBufferMapCollector<T,EX>::bind_contract_buffer_map(std::shared_ptr<ContractBufferMap<T,EX>> _contract_buffer_map){
    if(_contract_buffer_map){
        contract_buffer_map = _contract_buffer_map;
    }else{
        throw std::runtime_error("bind uninitialized contract_buffer_map to contract_buffer_map_collector");
    }
    return 0;
}

template<typename T, EXCHANGE EX>
int ContractBufferMapCollector<T,EX>::bind_contract_period_computer(
        std::shared_ptr<ContractPeriodComputer<T, EX>> _contract_period_computer) {
    if(_contract_period_computer){
        contract_period_computer = _contract_period_computer;
    }else{
        throw std::runtime_error("bind uninitialized contract_period_computer to contract_buffer_map_collector");
    }
    return 0;
}


template<typename T, EXCHANGE EX>
void ContractBufferMapCollector<T,EX>::init() {
    //int64_t latency;
    //int32_t active_threads;
    //absl::Time last_update_time;
    [this](){
        SystemParam system_param;
        system_param.init();
        json config = open_json_file("config/system.json");
        this->latency = system_param.collect_latency_ms;
        this->active_threads = system_param.active_threads;
        this->scale_rate = system_param.scale_rate;
        if(is_dual_NUMA()) {
            DLOG(INFO) << "Dual NUMA architecture detected.";
            if constexpr (EX == EXCHANGE::SH){
                node_arena = std::make_unique<tbb::task_arena>(active_threads, 0);
            }else if constexpr (EX == EXCHANGE::SZ){
                node_arena = std::make_unique<tbb::task_arena>(active_threads, 1);
            }else{
                throw std::runtime_error("Unknown type when partiation");
            }
        }else{
            DLOG(INFO) << "Single CPU or non-NUMA architecture detected.";
            int threads = tbb::info::default_concurrency();
            node_arena = std::make_unique<tbb::task_arena>(active_threads, 0);
        }
        this->min_period = config["system"]["min_period"];
        this->last_update_time = [&config,this](){
            //当日日期需要直接指定，因为无法判断hfq_table中，最后一天之后多久是下一个交易日，节假日的维护不稳定
            std::string str_day = config["system"]["date"];
            if (!absl::ParseCivilTime(str_day, &today)) {
                throw std::runtime_error("Fail to get today from system.json");
            }
            absl::Duration continuous_trading_begin = absl::Hours(9) + absl::Minutes(30) - absl::Milliseconds(1);
            absl::Time today_start = absl::FromCivil(today,sh_tz.tz);
            return today_start + continuous_trading_begin;
        }();
    }();

    //std::vector<std::array<char,11>> security_ids;
    security_ids = [](){
        SecurityId security_id_config;
        security_id_config.init();
        if constexpr (EX == EXCHANGE::SH){
            DLOG(INFO) << fmt::format("{} ----> size {}",str_type_ex<TradeInfo,EX>(),security_id_config.sh.size());
            return security_id_config.sh;
        }else if constexpr (EX == EXCHANGE::SZ){
            DLOG(INFO) << fmt::format("{} ----> size {}",str_type_ex<TradeInfo,EX>(),security_id_config.sz.size());
            return security_id_config.sz;
        }else{
            throw std::runtime_error("Invalid exchange type");
        }
    }();
}


template<typename T, EXCHANGE EX>
PackedInfoSp<T> ContractBufferMapCollector<T,EX>::collect_info_by_id(std::array<char,11>& security_id, absl::Time last_tp){
    auto iter = contract_buffer_map->security_id_to_contract_buffer_map.find(security_id);
    if(iter != contract_buffer_map->security_id_to_contract_buffer_map.end()){
        auto min_info_buffer_sp = iter->second.flush_by_time_threshold(last_tp);
        return min_info_buffer_sp;
    }else{
        throw std::runtime_error(fmt::format("security_id untracked"));
    }
}

/**
 * 休眠指定时间，并以指定时间进行收集
*/
template<typename T, EXCHANGE EX>
int ContractBufferMapCollector<T,EX>::special_signal_handler(absl::Time tp, int64_t sleep_time) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time/scale_rate + latency));
    tp += absl::Milliseconds(sleep_time);
    absl::CivilMinute truncated_civil = absl::ToCivilMinute(tp,sh_tz.tz);
    absl::Time truncated_time = absl::FromCivil(truncated_civil,sh_tz.tz);
    LOG(INFO) << fmt::format("Collect at truncated_time {} for {}", absl_time_to_str(truncated_time),str_type_ex<T,EX>());
    run_collect(truncated_time);
    return 0;
}

template<typename T, EXCHANGE EX>
int ContractBufferMapCollector<T,EX>::signal_handler(absl::Time tp) {
    std::lock_guard<std::mutex> sig_mtx(mtx);
    //TODO：等待时间
    //DEBUG
    std::this_thread::sleep_for(std::chrono::milliseconds(min_period/scale_rate + latency));
    //std::this_thread::sleep_for(std::chrono::milliseconds(latency));
    //TODO：修改为能兼容对齐取整模式
    tp += absl::Milliseconds(min_period);
    absl::CivilMinute truncated_civil = absl::ToCivilMinute(tp,sh_tz.tz);
    absl::Time truncated_time = absl::FromCivil(truncated_civil,sh_tz.tz);
    LOG(INFO) << fmt::format("Collect at truncated_time {} for {}", absl_time_to_str(truncated_time),str_type_ex<T,EX>());
    run_collect(truncated_time);
    return 0;
}

template<typename T, EXCHANGE EX>
int ContractBufferMapCollector<T,EX>::run_collect(absl::Time threshold_tp) {
    rec_count++;
    //parallel version
    node_arena->execute([&]{
        tbb::parallel_for(size_t(0), security_ids.size(), [&](size_t i) {
        auto this_period_info = collect_info_by_id(security_ids[i], threshold_tp);
        contract_period_computer->process_minimum_period_info(this_period_info,i,rec_count);
        });
    });
    // for(int i = 0; i < security_ids.size(); i++){
    //     auto this_period_info = collect_info_by_id(security_ids[i], threshold_tp);
    //     if(this_period_info->size() != 0){
    //         if(this_period_info->size() == 1){
    //             std::cout << i << std::endl;
    //             exit(0);
    //         }
    //         contract_period_computer->process_minimum_period_info(this_period_info,i,rec_count);
    //     }
    // }

    // for(int i = 0; i < security_ids.size(); i++){
    //     auto this_period_info = collect_info_by_id(security_ids[i], threshold_tp);
    //     contract_period_computer->process_minimum_period_info(this_period_info,i,rec_count);
    // }
    int64_t index_tp = absl::ToUnixMicros(threshold_tp + absl::Hours(8));
    contract_period_computer->aggregator_notify(rec_count,index_tp);
    return 0;
}
#endif