//
// Created by fanghz on 8/28/23.
//

#ifndef EMDS_PERIOD_SIGNAL_GENERATOR_HPP
#define EMDS_PERIOD_SIGNAL_GENERATOR_HPP
#include <memory>
#include <thread>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include <zmq.hpp>
#include "../common/info.hpp"
#include "../common/time_utils.hpp"
#include "../common/fmt_expand.hpp"
#include "../common/utils.hpp"
#include "../common/config_type.hpp"
#include "collector.hpp"
#include "collector_manager.hpp"
#include "../logger/async_logger.hpp"

template<typename T,EXCHANGE EX>
class PeriodSignalGenerator{
public:
    ZmqConfig zmq_cfg;
    std::shared_ptr<ContractBufferMapCollector<T,EX>> collector;
    LoggerPtr logger;
public:
    inline auto zmq_bind(){
        zmq::context_t context(1);
        zmq::socket_t subscriber(context, zmq::socket_type::sub);
        subscriber.set(zmq::sockopt::rcvbuf, 1024 * 1024);
        subscriber.set(zmq::sockopt::sndhwm, 0);
        subscriber.set(zmq::sockopt::subscribe,zmq_cfg.channel);
        subscriber.connect(zmq_cfg.to_bind_addr());
        return subscriber;
    }
    inline void minimal_period_signal_generator(std::stop_token stoken){
        auto subscriber = zmq_bind();
        absl::CivilDay today = collector->today;
        int send_count = 0;
        while (true) {
            zmq::message_t message;
            auto res_state = subscriber.recv(message, zmq::recv_flags::none);
            if(!res_state){
                logger->warn(fmt::format("Failed to rec messgae for PeriodSignalGenerator {}",str_type_ex<T,EX>()));
            }
            std::string message_str(static_cast<char*>(message.data()), message.size());
            std::vector<std::string_view> x = absl::StrSplit(message_str,",");
            int time_part_index = get_time_part_index();
            absl::Duration bias = convert_time_string_to_duration(x[time_part_index]);
            absl::Time today_start = absl::FromCivil(today,sh_tz.tz);
            absl::Time t_now = today_start + bias;
            if( is_next_civil_min(collector->last_update_time, t_now) ){
                std::jthread collect_task([this,t_now]() {
                    this->collector->signal_handler(t_now);
                });
                logger->info(fmt::format("{} Collection Signal Sended {} times",str_type_ex<T,EX>(),send_count));
                collect_task.detach();
            }
            collector->last_update_time = ( collector->last_update_time < t_now ) ? t_now : collector->last_update_time;
            if(stoken.stop_requested()){
                logger->info(fmt::format("Cancel PeriodSignalGenerator for {}", str_type_ex<T,EX>()));
                break;
            }
        }
    }
public:
    PeriodSignalGenerator();
    constexpr int get_time_part_index() const {
        if constexpr (std::is_same_v<T, TradeInfo>) {
            if constexpr (EX == EXCHANGE::SH) {
                return 4;
            } else if constexpr (EX == EXCHANGE::SZ) {
                return 11;
            }
        } else if constexpr (std::is_same_v<T, DepthInfo>) {
            if constexpr (EX == EXCHANGE::SH) {
                return 0;
            } else if constexpr (EX == EXCHANGE::SZ) {
                return 0;
            }
        } else if constexpr (std::is_same_v<T, OrderInfo>) {
            if constexpr (EX == EXCHANGE::SH) {
                return 0;
            } else if constexpr (EX == EXCHANGE::SZ) {
                return 0;
            }
        }else{
            return 0;
        }
    }
};

template<typename T, EXCHANGE EX>
PeriodSignalGenerator<T, EX>::PeriodSignalGenerator() {
    SubscribeConfig sub_cfg;
    sub_cfg.init();
    if constexpr (std::is_same_v<T, TradeInfo>) {
        if constexpr (EX == EXCHANGE::SH) {
            zmq_cfg = sub_cfg.sh_trade;
        } else if constexpr (EX == EXCHANGE::SZ) {
            zmq_cfg = sub_cfg.sz_trade;
        }
    } else if constexpr (std::is_same_v<T, DepthInfo>) {
        if constexpr (EX == EXCHANGE::SH) {
            zmq_cfg = sub_cfg.sh_depth;
        } else if constexpr (EX == EXCHANGE::SZ) {
            zmq_cfg = sub_cfg.sz_depth;
        }
    } else if constexpr (std::is_same_v<T, OrderInfo>) {
        if constexpr (EX == EXCHANGE::SH) {
            zmq_cfg = sub_cfg.sh_order;
        } else if constexpr (EX == EXCHANGE::SZ) {
            zmq_cfg = sub_cfg.sz_order;
        }
    }
}
#endif //EMDS_PERIOD_SIGNAL_GENERATOR_HPP
