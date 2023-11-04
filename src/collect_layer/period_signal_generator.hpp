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
    absl::Duration filter_boundary;
private:
    zmq::context_t context;       // ZMQ context as a member
    zmq::socket_t subscriber;     // ZMQ subscriber as a member
public:
    void zmq_bind(){
        try {
            subscriber.set(zmq::sockopt::rcvbuf, 1024 * 1024);
            subscriber.set(zmq::sockopt::rcvhwm, 0);
            subscriber.set(zmq::sockopt::subscribe, zmq_cfg.channel);
            subscriber.connect(zmq_cfg.to_bind_addr());
        } catch (const zmq::error_t& e) {
            LOG(ERROR) << "ZMQ Error: " << e.what();
        }
    }
    void minimal_period_signal_generator(std::stop_token stoken);
public:
    PeriodSignalGenerator();
    void register_collector(std::shared_ptr<ContractBufferMapCollector<T,EX>> sp){
        collector = sp;
    }
    void register_logger(const std::shared_ptr<LoggerManager>& logger_manager);
    [[nodiscard]] constexpr int get_time_part_index() const {
        if constexpr (std::is_same_v<T, TradeInfo>) {
            if constexpr (EX == EXCHANGE::SH) {
                return 4;
            } else if constexpr (EX == EXCHANGE::SZ) {
                return 10;
            }
        }
        //TODO 
        else if constexpr (std::is_same_v<T, DepthInfo>) {
            if constexpr (EX == EXCHANGE::SH) {
                return 0;
            } else if constexpr (EX == EXCHANGE::SZ) {
                return 0;
            }
        }
        else if constexpr (std::is_same_v<T, OrderInfo>) {
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
void PeriodSignalGenerator<T, EX>::register_logger(const std::shared_ptr<LoggerManager> &logger_manager) {
    logger = logger_manager->get_logger(fmt::format("PeriodSignalGenerator_{}", str_type_ex<TradeInfo,EX>()));
}

template<typename T, EXCHANGE EX>
PeriodSignalGenerator<T, EX>::PeriodSignalGenerator():context(1), subscriber(context, zmq::socket_type::sub){
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
    filter_boundary = absl::Hours(9) + absl::Minutes(30);
}

template<typename T,EXCHANGE EX>
void PeriodSignalGenerator<T, EX>::minimal_period_signal_generator(std::stop_token stoken) {
    zmq_bind();
    absl::CivilDay today = collector->today;
    int send_count = 0;
    bool is_first_trade_message = true;
    while (true) {
        zmq::message_t message;
        auto res_state = subscriber.recv(message, zmq::recv_flags::none);
        if(!res_state){
            logger->warn(fmt::format("Failed to rec message for period signal generator {}",str_type_ex<T,EX>()));
        }
        std::string message_str(static_cast<char*>(message.data()), message.size());
        std::vector<std::string_view> x = absl::StrSplit(message_str,",");
        int time_part_index = get_time_part_index();
        absl::Duration bias = convert_time_string_to_duration(x[time_part_index]);
        absl::Time today_start = absl::FromCivil(today,sh_tz.tz);
        absl::Time t_now = today_start + bias;
        //特殊判断，如果是首次获得9点15分的Trade消息，则抛出两个信号
        if(is_first_trade_message){
            /**
             * 9:15:xx + 10min to 9:25 min
            */
            std::jthread collect_task_for_call_auction([this,t_now](){
                this->collector->special_signal_handler(t_now,10*60*1000);
            });
            send_count++;
            LOG(INFO) << fmt::format("Special_signal generator for {} call_auction at 09:25:00",str_type_ex<T,EX>());
            collect_task_for_call_auction.detach(); 
            
            /**
             * 9:15:xx + 15min to 9:30
            */
            std::jthread collect_task_for_market_open([this,t_now](){
                this->collector->special_signal_handler(t_now,15*60*1000);
            });
            send_count++;
            LOG(INFO) << fmt::format("Special_signal generator for {} market_open at 09:30:00",str_type_ex<T,EX>());
            collect_task_for_market_open.detach();

            is_first_trade_message = false; 
        }
        //filter the trade message in (,9:30:00.000]
        if( bias > filter_boundary ){
            if(is_next_civil_min(collector->last_update_time, t_now)){
                std::jthread collect_task([this,t_now]() {
                    this->collector->signal_handler(t_now);
                });
                send_count++;
                LOG(INFO) << fmt::format("From {} to {}", absl_time_to_str(collector->last_update_time),absl_time_to_str(t_now));
                LOG(INFO) << fmt::format("Period signal generator {} send {} times",str_type_ex<T,EX>(),send_count);
                logger->info(fmt::format("From {} to {}", absl_time_to_str(collector->last_update_time),absl_time_to_str(t_now)));
                logger->info(fmt::format("Period signal generator {} send {} times",str_type_ex<T,EX>(),send_count));
                collect_task.detach();
            }
            collector->last_update_time = ( collector->last_update_time < t_now ) ? t_now : collector->last_update_time;
        }
        if(stoken.stop_requested()){
            DLOG(INFO) << fmt::format("Cancel period signal generator {}", str_type_ex<T,EX>());
            logger->info(fmt::format("Cancel period signal generator {}", str_type_ex<T,EX>()));
            break;
        }
    }
}
#endif //EMDS_PERIOD_SIGNAL_GENERATOR_HPP
