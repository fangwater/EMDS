#ifndef CONTRACT_COLLECTOR_HPP
#define CONTRACT_COLLECTOR_HPP
#include <memory>
#include <omp.h>
#include "../cache_layer/contract_buffer.hpp"
#include "../common/info.hpp"
#include <thread>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include "../common/time_utils.hpp"
#include "../common/fmt_expand.hpp"
#include "../common/utils.hpp"
#include "../common/config_type.hpp"
#include "../aggregate_layer/period_result.hpp"
#include "../compute_lib/candle_stick.hpp"
#include "../common/callback_factory.hpp"
#include "../aggregate_layer/period_result.hpp"
#include "../aggregate_layer/aggregator_manager.hpp"

template<typename T>
class PeriodContext{
public:
    int period;
    PackedInfoSp<T> accumulated_info;
    std::vector<CallBackObjPtr<T>> callbacks;
    std::vector<std::string> callback_names;
    std::size_t append_infos(PackedInfoSp<T> src){
        accumulated_info->reserve(accumulated_info->size() + src->size());
        std::copy(src->begin(), src->end(), std::back_inserter(*accumulated_info));
        return accumulated_info->size();
    }
};

template<typename T>
class ContractBufferMapCollector{
public:
    std::string exchange_type;
    std::shared_ptr<ContractBufferMap<T>> contract_buffer_map;
    std::shared_ptr<AggregatorManager> aggreator_sp;
    std::vector<std::array<char,11>> security_ids;
    mutable std::mutex mtx;
    int64_t latency;
    int32_t active_threads;
    absl::Time last_update_time;
    std::vector<PeriodContext<T>> per_period_ctx;
    int32_t rec_count;
public:
    ContractBufferMapCollector(std::string shanghai_or_shenzhen);
public:
    int signal_handler(absl::Time t);
    int init();
    int register_aggretor_manager(std::shared_ptr<AggregatorManager> sp);
public:
    PackedInfoSp<T> collect_info_by_id(std::array<char,11>& security_id, absl::Time last_tp);
    //int register_callback(std::string& f_name, CallBackObjPtr<T> callback_obj_ptr, std::vector<int>& open_periods);
    int run_collect(absl::Time t);
};

//上海和深圳会汇总到一个数据表
template<typename T>
int ContractBufferMapCollector<T>::register_aggretor_manager(std::shared_ptr<AggregatorManager> sp) {
    aggreator_sp = sp;
}

template<typename T>
ContractBufferMapCollector<T>::ContractBufferMapCollector(std::string shanghai_or_shenzhen)
    :rec_count(0),exchange_type(shanghai_or_shenzhen){};

template<typename T>
int ContractBufferMapCollector<T>::init() {
    //int64_t latency;
    //int32_t active_threads;
    //absl::Time last_update_time;
    [this](){
        json config = open_json_file("config/system.json");
        this->latency = config["system"]["collect_latency(ms)"];
        this->active_threads = config["system"]["active_threads"];
        this->last_update_time = [&config,this](){
            absl::CivilDay today;
            //当日日期需要直接指定，因为无法判断hfq_table中，最后一天之后多久是下一个交易日，节假日的维护不稳定
            std::string str_day = config["system"]["date"];
            if (!absl::ParseCivilTime(str_day, &today)) {
                throw std::runtime_error("Fail to get today from system.json");
            }
            absl::Duration continuous_trading_begin = absl::Hours(9) + absl::Minutes(30);
            absl::Time today_start = absl::FromCivil(today,sh_tz.tz);
            return today_start + continuous_trading_begin - absl::Milliseconds(this->latency);
        }();
    }();

    //初始化每个周期需要调用的函数
    //std::vector<PeriodContext<T>> per_period_ctx;
    if constexpr (std::is_same_v<T,TradeInfo>){
        [this](){
            PeriodCtxConfig trade_ctx_config(CtxType::TRADE);
            trade_ctx_config.init();
            //构造每个周期的Ctx
            for(int i = 0; i < trade_ctx_config.unique_periods.size(); i++){
                PeriodContext<TradeInfo> period_ctx;
                period_ctx.period = trade_ctx_config.unique_periods.at(i);
                period_ctx.callback_names = trade_ctx_config.per_period_feature_name_list.at(i);
                for(auto& cb_name : period_ctx.callback_names){
                    CallBackObjFactory factory;
                    auto callback_obj_ptr = factory.create_trade_function_callback(cb_name,exchange_type);
                    period_ctx.callbacks.push_back(std::move(callback_obj_ptr));
                }
                period_ctx.accumulated_info = [](){
                    std::vector<std::shared_ptr<TradeInfo>> vec;
                    return std::make_shared<std::vector<std::shared_ptr<TradeInfo>>>(std::move(vec));
                }();
                this->per_period_ctx.push_back(std::move(period_ctx));
            }
        }();
    }else if constexpr (std::is_same_v<T,OrderInfo>){
        //TODO
    }else if constexpr (std::is_same_v<T,DepthInfo>){
        //TODO
    }else{
        throw std::runtime_error("Unexpected Info Type");
    }
    //std::vector<std::array<char,11>> security_ids;
    security_ids = [this](){
        SecurityId security_id_config;
        security_id_config.init();
        if(this->exchange_type == "sz"){
            return security_id_config.sz;
        }else if(this->exchange_type == "sh"){
            return security_id_config.sh;
        }
    }();
    //std::shared_ptr<ContractBufferMap<T>> contract_buffer_map;
    contract_buffer_map = std::make_shared<ContractBufferMap<T>>(security_ids,8192);
}

template<typename T>
PackedInfoSp<T> ContractBufferMapCollector<T>::collect_info_by_id(std::array<char,11>& security_id, absl::Time last_tp){
    auto iter = contract_buffer_map->securiy_id_to_contract_buffer_map.find(security_id);
    if(iter != contract_buffer_map->securiy_id_to_contract_buffer_map.end()){
        auto min_info_buffer_sp = iter->second.flush_by_time_threshold(last_tp);
        return min_info_buffer_sp;
    }else{
        throw std::runtime_error(fmt::format("{} is untracked security_id ",security_id));
    }
}

template<typename T>
int ContractBufferMapCollector<T>::signal_handler(absl::Time tp) {
    std::lock_guard<std::mutex> sig_mtx(mtx);
    std::this_thread::sleep_for(std::chrono::milliseconds(latency));
    //TODO：修改为能兼容对齐取整模式
    absl::CivilMinute truncated_civil = absl::ToCivilMinute(tp,sh_tz.tz);
    absl::Time truncated_time = absl::FromCivil(truncated_civil,sh_tz.tz);
    run_collect(truncated_time);
}

template<typename T>
int ContractBufferMapCollector<T>::run_collect(absl::Time threshold_tp) {
    threshold_tp += absl::Milliseconds(latency);
    rec_count++;
    omp_set_num_threads(active_threads);
    #pragma omp parallel for
    for(int i = 0; i < security_ids.size(); i++){
        auto this_period_info = collect_info_by_id(security_ids[i], threshold_tp);
        for(auto& period_ctx : per_period_ctx){
            period_ctx.append_infos(this_period_info);
            if(!rec_count % period_ctx.period){
                //收集此周期所注册feature函数的计算结果
                std::vector<std::vector<double>> callback_results;
                //顺次调用注册的callback函数，完成当前period所需的计算
                for(int j = 0; j < period_ctx.callbacks.size(); j++){
                    auto res = period_ctx.callbacks.at(j)->calculate(this_period_info,i);
                    callback_results.push_back(std::move(res));
                }
                PeriodResult period_res{
                        .period = period_ctx.period,
                        .security_id = security_ids[i],
                        .results = std::move(callback_results)
                };
                aggreator_sp->commit(std::move(period_res));
                //释放当前period的数据
                period_ctx.accumulated_info->clear();
            }
        }
    }
}

//droped
//template<typename T>
//int ContractBufferMapCollector<T>::register_callback(
//        std::string &f_name,
//        CallBackObjPtr<T> callback_obj_ptr,
//        std::vector<int> &open_periods) {
//    for(int i = 0; i < open_periods.size(); i++){
//        auto iter = period_to_ctx_index.find(open_periods[i]);
//        if(iter == period_to_ctx_index.end()){
//            throw std::runtime_error(fmt::format("{} period is not inited.", open_periods[i]));
//        }
//        int index = iter->second;
//        auto& ctx = per_period_ctx[index];
//        ctx.callback_names.emplace_back(f_name);
//        ctx.callbacks.push_back(std::move(callback_obj_ptr));
//        LOG(INFO) << fmt::format("Register function {} for period {}", f_name,ctx.period);
//    }
//    return 0;
//}

#endif