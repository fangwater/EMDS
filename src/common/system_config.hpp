#ifndef SYSTEM_PARAM
#define SYSTEM_PARAM
#include "utils.hpp"
#include <absl/time/time.h>
#include <absl/time/civil_time.h>
class SystemParam{
public:
    int collect_latency_ms;
    absl::CivilDay today;
    void init(){
        json config = open_json_file("config/system.json");
        //获取当天的日期，后续操作依赖于这个函数
        today = [&config](){
            absl::CivilDay today;
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
    }
};
#endif
