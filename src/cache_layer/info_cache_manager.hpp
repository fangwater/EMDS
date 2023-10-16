#ifndef INFO_CACHE_MANAGER_HPP
#define INFO_CACHE_MANAGER_HPP

#include <zmq.hpp>
#include "../common/fmt_expand.hpp"
#include "trade_info_cache.hpp"
#include "parser/trade_parser.hpp"

//TODO: market, order

class TradeInfoCacheManager{
public:
    LoggerPtr logger;
    std::shared_ptr<TradeInfoCache<EXCHANGE::SH>> sh_trade_info_cache;
    std::shared_ptr<TradeInfoCache<EXCHANGE::SZ>> sz_trade_info_cache;
private:
    std::vector<std::jthread> receivers;
private:
    template<EXCHANGE EX>
    void trade_info_zmq_receiver(std::stop_token stoken);

    template<EXCHANGE EX>
    void create_receiver() {
        receivers.emplace_back([this](std::stop_token st) {
            trade_info_zmq_receiver<EX>(st);
        });
    }
public:
    ~TradeInfoCacheManager() {
        logger->info("Destructing TradeInfoCacheManager...");
        LOG(INFO) << "Destructing TradeInfoCacheManager..."; 
        for (auto& t : receivers) {
            if (t.joinable()) {
                t.request_stop();
            }
        }
        logger->info("TradeInfoCacheManager destructed.");
        LOG(INFO) << "TradeInfoCacheManager destructed."; 
    }

    void run_deliver_threads(int k){
        sz_trade_info_cache->init_submit_threads(k);
        sh_trade_info_cache->init_submit_threads(k);
    }
    explicit TradeInfoCacheManager(const std::shared_ptr<LoggerManager>& logger_manager){
        logger = logger_manager->get_logger("TradeInfoCacheManager");
        logger->info(fmt::format("Create TradeInfoCacheManager at {}",get_current_time_as_string()));

        SystemParam system_param;
        system_param.init();
        absl::CivilDay today = system_param.today;
        sh_trade_info_cache = std::make_shared<TradeInfoCache<EXCHANGE::SH>>(system_param.cache_size,today);
        sh_trade_info_cache->register_logger(logger_manager);
        sz_trade_info_cache = std::make_shared<TradeInfoCache<EXCHANGE::SZ>>(system_param.cache_size,today);
        sz_trade_info_cache->register_logger(logger_manager);

        //为cache创造buffer,size不用过大，因为满足最小周期以后就会被收走
        sz_trade_info_cache->init_contract_buffer_map();
        sh_trade_info_cache->init_contract_buffer_map();

        //开启深圳，上海的Trade缓存模块，并初始化
        logger->info(fmt::format("Success create cache queue in TradeInfoCacheManager for sz and sh"));
        SubscribeConfig sub_config;
        sub_config.init();

        //创建TradeCache的parser,需要先有parser,才能调用parser

        sh_trade_info_cache->init_message_parser();
        logger->info("Construct parser for {}", str_type_ex<TradeInfo,EXCHANGE::SH>());
        create_receiver<EXCHANGE::SH>();
        logger->info("Start trade data receiver for {}", str_type_ex<TradeInfo,EXCHANGE::SH>());

        sz_trade_info_cache->init_message_parser();
        logger->info("Construct parser for {}", str_type_ex<TradeInfo,EXCHANGE::SZ>());
        create_receiver<EXCHANGE::SZ>();
        logger->info("Start trade data receiver for {}", str_type_ex<TradeInfo,EXCHANGE::SZ>());

    }
};

template<EXCHANGE EX>
void TradeInfoCacheManager::trade_info_zmq_receiver(std::stop_token stoken) {
    zmq::context_t context(1);
    zmq::socket_t subscriber(context, zmq::socket_type::sub);
    subscriber.set(zmq::sockopt::rcvbuf, 1024 * 1024);
    subscriber.set(zmq::sockopt::sndhwm, 0);
    SubscribeConfig sub_config;
    sub_config.init();
    if constexpr (EX == EXCHANGE::SH){
        subscriber.set(zmq::sockopt::subscribe,sub_config.sh_trade.channel);
        subscriber.connect(sub_config.sh_trade.to_bind_addr());
    }else if constexpr (EX == EXCHANGE::SZ){
        subscriber.set(zmq::sockopt::subscribe,sub_config.sz_trade.channel);
        subscriber.connect(sub_config.sz_trade.to_bind_addr());
    }
    while (true) {
        zmq::message_t message;
        auto res_state = subscriber.recv(message, zmq::recv_flags::none);
        if(!res_state){
            logger->warn("Failed to get message for {} at {}", str_type_ex<TradeInfo,EX>(), get_current_time_as_string());
        }
        std::string message_str(static_cast<char*>(message.data()), message.size());
        if constexpr (EX == EXCHANGE::SH){
            sh_trade_info_cache->parser->MessageProcess(std::string_view(message_str));
        }else if constexpr (EX == EXCHANGE::SZ){
            sz_trade_info_cache->parser->MessageProcess(std::string_view(message_str));
        }
        if(stoken.stop_requested()){
            logger->info("Cancel parser for {} at {}", str_type_ex<TradeInfo,EX>(), get_current_time_as_string());
            break;
        }
    }
}

#endif //INFO_CACHE_MANAGER_HPP