#ifndef CONFIG_TYPE
#define CONFIG_TYPE
#include "utils.hpp"
#include <absl/time/time.h>
#include <absl/time/civil_time.h>
class SystemParam{
public:
    int collect_latency_ms;
    absl::CivilDay today;
    int active_threads;
    int64_t contract_buffer_size;
    int64_t cache_size;
    void init(){
        json config = open_json_file("config/system.json");
        //获取当天的日期，后续操作依赖于这个函数
        today = [&config,this](){
            /**
             * 当日日期需要直接指定，因为无法判断hfq_table中，最后一天之后多久是下一个交易日，节假日的维护不稳定
             * */
            std::string str_day = config["system"]["date"];
            if (!absl::ParseCivilTime(str_day, &today)) {
                throw std::runtime_error("Fail to get today from system.json");
            }
            return today;
        }();
        collect_latency_ms = config["system"]["collect_latency(ms)"];
        active_threads = config["system"]["active_threads"];
        contract_buffer_size = config["system"]["contract_buffer_size"];
        cache_size = config["system"]["cache_size"];
    }
};


enum class CtxType{
    TRADE,
    ORDER,
    DEPTH
};

class PeriodCtxConfig{
public:
    CtxType type_;
    std::string ctx_type_string;
    std::vector<int> unique_periods;
    std::vector<std::vector<std::string>> per_period_feature_name_list;
    explicit PeriodCtxConfig(CtxType type):type_(type){
        if(type == CtxType::TRADE){
            ctx_type_string = "trade";
        }else if(type == CtxType::ORDER){
            ctx_type_string = "order";
        }else{
            ctx_type_string = "depth";
        }
    };
    void init(){
        json config = open_json_file("config/publish.json");
        std::vector<std::vector<int>> feature_periods_list;
        std::vector<std::string> feature_name_list;
        for(auto& function_ctx_json : config[ctx_type_string]){
            if(function_ctx_json["open"] == true){
                std::vector<int> this_feature_period = function_ctx_json["period"];
                feature_periods_list.push_back(std::move(this_feature_period));
                feature_name_list.push_back(function_ctx_json["name"]);
            }
        }
        //组织合并，以最小公共周期重排
        [&feature_name_list,&feature_periods_list,this](){
            auto res = K_way_ordered_vec_merge(feature_periods_list);
            this->unique_periods = res.first;
            this->per_period_feature_name_list.resize(unique_periods.size());
            for(int i = 0; i < res.second.size(); i++){
                auto function_name = feature_name_list[i];
                auto to_register = res.second[i];
                for(auto& index : to_register){
                    this->per_period_feature_name_list[index].push_back(function_name);
                }
            }
        }();
    }
};

class ClosePrice{
public:
    std::vector<double> sh;
    std::vector<double> sz;
    std::vector<double> all;
    void init(){
        json hfq_output = json::parse(std::ifstream("config/traced_contract.json"));
        sh = [&hfq_output](){
            std::vector<double> sz_security_closeprices = hfq_output["sz_closeprice"];
            return sz_security_closeprices;
        }();
        sz = [&hfq_output](){
            std::vector<double> sh_security_closeprices = hfq_output["sh_closeprice"];
            return sh_security_closeprices;
        }();
        all = vecMerge(sz,sh);
    }
};

class SecurityId{
public:
    std::vector<std::array<char,11>> sh;
    std::vector<std::array<char,11>> sz;
    std::vector<std::array<char,11>> all;
    void init(){
        json hfq_output = json::parse(std::ifstream("traced_contract.json"));
        sz = [&hfq_output](){
            std::vector<std::string> sz_securitys_str = hfq_output["sz"];
            return vecConvertStrToArray<11>(sz_securitys_str);
        }();
        sh = [&hfq_output](){
            std::vector<std::string> sz_securitys_str = hfq_output["sh"];
            return vecConvertStrToArray<11>(sz_securitys_str);
        }();
        all = vecMerge(sz,sh);
    }
};

class ZmqConfig{
public:
    std::string ip;
    int64_t port;
    std::string channel;
    std::string to_bind_addr(){
        return fmt::format("tcp://{}:{}",ip,port);
    }
};

class SubscribeConfig{
public:
    ZmqConfig sz_trade;
    ZmqConfig sh_trade;
    ZmqConfig sz_order;
    ZmqConfig sh_order;
    ZmqConfig sz_depth;
    ZmqConfig sh_depth;
public:
    void init() {
        nlohmann::json subscribe_config = open_json_file("config/subscribe.json");
        // sh_trade
        sh_trade.ip = subscribe_config["trade"]["sh"]["ip"];
        sh_trade.port = subscribe_config["trade"]["sh"]["port"];
        sh_trade.channel = subscribe_config["trade"]["sh"]["channel"];
        // sz_trade
        sz_trade.ip = subscribe_config["trade"]["sz"]["ip"];
        sz_trade.port = subscribe_config["trade"]["sz"]["port"];
        sz_trade.channel = subscribe_config["trade"]["sz"]["channel"];
        // sh_order
        sh_order.ip = subscribe_config["order"]["sh"]["ip"];
        sh_order.port = subscribe_config["order"]["sh"]["port"];
        sh_order.channel = subscribe_config["order"]["sh"]["channel"];
        // sz_order
        sz_order.ip = subscribe_config["order"]["sz"]["ip"];
        sz_order.port = subscribe_config["order"]["sz"]["port"];
        sz_order.channel = subscribe_config["order"]["sz"]["channel"];
        // sh_depth
        sh_depth.ip = subscribe_config["depth"]["sh"]["ip"];
        sh_depth.port = subscribe_config["depth"]["sh"]["port"];
        sh_depth.channel = subscribe_config["depth"]["sh"]["channel"];
        // sz_depth
        sz_depth.ip = subscribe_config["depth"]["sz"]["ip"];
        sz_depth.port = subscribe_config["depth"]["sz"]["port"];
        sz_depth.channel = subscribe_config["depth"]["sz"]["channel"];
    }
};


#endif
