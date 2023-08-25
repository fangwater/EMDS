#ifndef INFO_CACHE_MANAGER_HPP
#define INFO_CACHE_MANAGER_HPP

#include <zmq.hpp>
#include "common/fmt_expand.hpp"
#include "trade_info_cache.hpp"
#include "parser/trade_parser.hpp"

void TradeInfoZmqReceiver(std::stop_token stoken, std::shared_ptr<Parser<TradeInfo>> parser, std::string bind_to, std::string channel){
    zmq::context_t context(1);
    zmq::socket_t subscriber(context, zmq::socket_type::sub);
    //172.16.30.12 13002 sz_trade
    subscriber.set(zmq::sockopt::rcvbuf, 1024 * 1024);
    subscriber.set(zmq::sockopt::sndhwm, 0);
    subscriber.set(zmq::sockopt::subscribe,channel);
    subscriber.connect(bind_to);
    while (true) {
        zmq::message_t message;
        auto res_state = subscriber.recv(message, zmq::recv_flags::none);
        if(!res_state){
            LOG(WARNING) << "Failed to get message";
        }
        std::string message_str(static_cast<char*>(message.data()), message.size());
        parser->MessageProcess(std::string_view(message_str));
        if(stoken.stop_requested()){
            LOG(INFO) << "Cancel TickInfoZmqReceiver";
            break;
        }
    }
}

//TODO: market, order

class TradeInfoCacheManager{
public:
    std::shared_ptr<TradeInfoCache> sh_trade_info_cache;
    std::shared_ptr<TradeInfoCache> sz_trade_info_cache;
private:
    std::vector<std::jthread> receivers;
private:
    void create_receiver(std::shared_ptr<Parser<TradeInfo>> parser, std::string bind_to, std::string channel) {
        receivers.emplace_back([parser,&bind_to,&channel](std::stop_token st) {
            TradeInfoZmqReceiver(st, parser, bind_to, channel);
        });
    }
    void run_deliver_threadpool(int k){
        sh_trade_info_cache->init_submit_threadpool(k);
        sz_trade_info_cache->init_submit_threadpool(k);
    }
public:
    explicit TradeInfoCacheManager(absl::CivilDay today, std::vector<std::array<char,11>>& sh_security_ids, std::vector<std::array<char,11>>& sz_security_ids){
        json subscribe_config = open_json_file("config/subscribe.json");
        //开启深圳，上海的Trade缓存模块，并初始化
        sh_trade_info_cache = std::make_shared<TradeInfoCache>(1024*8*4096,today,"SH_Trade_Cache");
        sz_trade_info_cache = std::make_shared<TradeInfoCache>(1024*8*4096,today,"SZ_Trade_Cache");
        //创建TradeCache的parser
        sh_trade_info_cache->init_message_parser("SH");
        LOG(INFO) << "Construct parser for shanghai_trade";
        [&subscribe_config,this](){
            std::string bind_to = [&subscribe_config](){
                std::string ip = subscribe_config["trade"]["sh"]["ip"];
                int port = subscribe_config["trade"]["sh"]["port"];
                return fmt::format("tcp://{}:{}",ip,port);
            }();
            std::string channel =  subscribe_config["trade"]["sh"]["channel"];
            this->create_receiver(this->sh_trade_info_cache->parser, bind_to, channel);
            LOG(INFO) << "Start trade data receiver for shanghai";
        }();

        sz_trade_info_cache->init_message_parser("SZ");
        LOG(INFO) << "Construct parser for shenzhen_trade";
        [&subscribe_config,this](){
            std::string bind_to = [&subscribe_config](){
                std::string ip = subscribe_config["trade"]["sz"]["ip"];
                int port = subscribe_config["trade"]["sz"]["port"];
                return fmt::format("tcp://{}:{}",ip,port);
            }();
            std::string channel =  subscribe_config["trade"]["sz"]["channel"];
            this->create_receiver(this->sz_trade_info_cache->parser, bind_to, channel);
            LOG(INFO) << "Start trade data receiver for shenzhen";
        }();
        //为cache创造buffer,size不用过大，因为满足最小周期以后就会被收走
        sz_trade_info_cache->init_contract_buffer_map(sh_security_ids,8192*2);
        sh_trade_info_cache->init_contract_buffer_map(sz_security_ids,8192*2);
    }
};
#endif //INFO_CACHE_MANAGER_HPP