#ifndef CONTRACT_COLLECTOR_HPP
#define CONTRACT_COLLECTOR_HPP
#include <memory>
#include "../cache_layer/contract_buffer.hpp"
#include "../common/info.hpp"
#include <thread>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include "../common/time_utils.hpp"
#include "../common/fmt_expand.hpp"
#include "../common/utils.hpp"
#include "../common/system_config.hpp"

template<typename T>
using packed_info_sp = std::shared_ptr<std::vector<std::shared_ptr<T>>>;

template<typename T>
using feature_func = std::function<std::vector<double>(packed_info_sp<T>)>;


template<typename T>
class PeriodContext{
public:
    int period;
    std::vector<packed_info_sp<T>> info;
    std::vector<feature_func<T>> callbacks;
    std::vector<std::string> callback_names;
};

template<typename T>
class ContractBufferMapCollector{
public:
    std::shared_ptr<ContractBufferMap<T>> contract_buffer_map;
    std::vector<std::array<char,11>> security_ids;
    mutable std::mutex mtx;
    int64_t latency;
    absl::Time last_update_time;
    std::vector<PeriodContext<T>> per_period_ctx;
    absl::flat_hash_map<int,int> period_to_ctx_index;
public:
    int signal_handler(absl::Time t);
public:
    packed_info_sp<T> collect_info_by_id(std::array<char,11>& security_id, absl::Time last_tp);
    int register_callback(std::string& f_name, feature_func<T> func, std::vector<int>& open_periods);
    int run_collect(absl::Time t);
};

template<typename T>
packed_info_sp<T> ContractBufferMapCollector<T>::collect_info_by_id(std::array<char,11>& security_id, absl::Time last_tp){
    auto iter = contract_buffer_map->securiy_id_to_contract_buffer_map.find(security_id);
    if(iter != contract_buffer_map->securiy_id_to_contract_buffer_map.end()){
        auto min_info_buffer_sp = iter->second.flush_by_time_threshold(last_tp);
        return min_info_buffer_sp;
    }else{
        throw std::runtime_error(fmt::format("{} is untracked security_id ",security_id));
    }
}

template<typename T>
int ContractBufferMapCollector<T>::register_callback(
        std::string &f_name,
        feature_func<T> func,
        std::vector<int> &open_periods) {
    for(int i = 0; i < open_periods.size(); i++){
        auto iter = period_to_ctx_index.find(open_periods[i]);
        if(iter == period_to_ctx_index.end()){
            throw std::runtime_error(fmt::format("{} period is not inited.", open_periods[i]));
        }
        int index = iter->second;
        auto& ctx = per_period_ctx[index];
        ctx.callback_names.emplace_back(f_name);
        ctx.callbacks.emplace_back(func);
        LOG(INFO) << fmt::format("Register function {} for period {}", f_name,ctx.period);
    }
    return 0;
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

#endif