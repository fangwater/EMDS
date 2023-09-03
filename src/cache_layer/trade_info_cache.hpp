#ifndef TRADE_INFO_CACHE_MANAGER_HPP
#define TRADE_INFO_CACHE_MANAGER_HPP

#include "info_cache.hpp"
#include "parser/trade_parser.hpp"
template<EXCHANGE EX>
class TradeInfoCache : public InfoCache<TradeInfo,EX>{
public:
    std::shared_ptr<Parser<TradeInfo,EX>> parser;
public:
    void init_message_parser() override{
        if constexpr (EX == EXCHANGE::SH){
            parser = std::make_shared<SHTradeInfoParser>(this->shared_from_this());
        }else if constexpr (EX == EXCHANGE::SZ){
            parser = std::make_shared<SZTradeInfoParser>(this->shared_from_this());
        }else{
            throw std::runtime_error( fmt::format("Fail to create parser for {}", str_type_ex<TradeInfo,EX>()));
        }
    }

    int init_contract_buffer_map() override{
            this->security_id_to_buffer_map_sp = std::make_shared<ContractBufferMap<TradeInfo,EX>>();
            if(this->security_id_to_buffer_map_sp){
                this->logger->info(fmt::format("Success contract buffer for info_cache {}", str_type_ex<TradeInfo,EX>()));
                return 0;
            }else{
                throw std::runtime_error(fmt::format("Fail to create security buffer for info_cache {}", str_type_ex<TradeInfo,EX>()));
            }
    }
    TradeInfoCache(std::size_t queue_size, absl::CivilDay today)
            : InfoCache<TradeInfo,EX>(queue_size, today) {}
};
#endif