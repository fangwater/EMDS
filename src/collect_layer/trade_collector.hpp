#ifndef TRADE_CONTRACT_COLLECTOR_HPP
#define TRADE_CONTRACT_COLLECTOR_HPP
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


class TradeCollectorManager : public CollectorManager<TradeInfo>{
public:
public:
    TradeCollectorManager(std::shared_ptr<AggregatorManager> aggregator_sp):
    CollectorManager<TradeInfo>(aggregator_sp){
        LOG(INFO) << fmt::format("Creating TradeCollectorManager");
    }
};

template<typename T,EXCHANGE EX>
class PeriodSignalGenerator{
public:

private:
    std::shared_ptr<ContractBufferMapCollector<T,EX>> collector_sp;
    void sh_trade(){
        zmq::context_t context(1);
        zmq::socket_t subscriber(context, zmq::socket_type::sub);
        subscriber.set(zmq::sockopt::rcvbuf, 1024 * 1024);
        subscriber.set(zmq::sockopt::sndhwm, 0);
        subscriber.set(zmq::sockopt::subscribe,"");
        subscriber.connect(bind_to);
    }
public:

    void minimal_period_signal_generator(){

    }
}
void sh_minimal_period_signal_generator()
// void SH_trade_period_signal_generator(absl::CivilDay today, std::stop_token stoken, std::string bind_to, std::shared_ptr<ContractBufferMapCollector<TradeInfo>> collector_sp){
//     zmq::context_t context(1);
//     zmq::socket_t subscriber(context, zmq::socket_type::sub);
//     subscriber.set(zmq::sockopt::rcvbuf, 1024 * 1024);
//     subscriber.set(zmq::sockopt::sndhwm, 0);
//     subscriber.set(zmq::sockopt::subscribe,"");
//     subscriber.connect(bind_to);
//     while (true) {
//         zmq::message_t message;
//         auto res_state = subscriber.recv(message, zmq::recv_flags::none);
//         if(!res_state){
//             std::cerr << "Failed to rec messgae" << std::endl;
//         }
//         std::string message_str(static_cast<char*>(message.data()), message.size());
//         std::vector<std::string_view> x = absl::StrSplit(message_str,",");
//         absl::Duration bias = convert_time_string_to_duration(x[4]);
//         absl::Time today_start = absl::FromCivil(today,sh_tz.tz);
//         absl::Time t_now = today_start + bias;
//         if( is_next_civil_min(collector_sp->last_update_time,t_now) ){
//             std::jthread collect_task([collector_sp,t_now]() {
//                 collector_sp->run_min_period_collect(t_now);
//             });
//             LOG(INFO) << "SH Trade Collection Signal Sended";
//             collect_task.detach();
//         }
//         collector_sp->last_update_time = ( collector_sp->last_update_time < t_now ) ? t_now : collector_sp->last_update_time;
//         if(stoken.stop_requested()){
//             LOG(INFO) << "Cancel SH_trade_period_signal_generator";
//             break;
//         }
//     }
// }

#endif