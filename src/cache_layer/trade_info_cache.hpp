#ifndef TRADE_INFO_CACHE_MANAGER_HPP
#define TRADE_INFO_CACHE_MANAGER_HPP

#include "info_cache.hpp"
class TradeInfoCache : public InfoCache<TradeInfo>{
public:
    std::shared_ptr<Parser<TradeInfo>> parser;
public:
    void init_message_parser(std::string&& parser_type) override{
        if(parser_type == "SH"){
            parser = std::make_shared<SHTradeInfoParser>(this->shared_from_this());
            LOG(INFO) << fmt::format("Created {} parser for {}","sh_tick", info_cache_name);
        }else if(parser_type == "SZ"){
            parser = std::make_shared<SZTradeInfoParser>(this->shared_from_this());
            LOG(INFO) << fmt::format("Created {} parser for {}","sz_tick", info_cache_name);
        }else{
            LOG(FATAL) << fmt::format("Fail to create parser for {}",info_cache_name);
        }
    }
    int init_contract_buffer_map(std::vector<std::array<char,11>>& security_ids, std::size_t buffer_size) override{
            security_id_to_buffer_map_sp = std::make_shared<ContractBufferMap<TradeInfo>>(security_ids, buffer_size);
            if(security_id_to_buffer_map_sp){
                LOG(INFO) << "Success SecurityBuffer for info_cache " << info_cache_name;
                return 0;
            }else{
                LOG(FATAL) << "Fail to create security buffer for info_cache "<< info_cache_name;
            }
            return -1;
    }
    TradeInfoCache(std::size_t queue_size, absl::CivilDay today, std::string&& name)
            : InfoCache<TradeInfo>(queue_size, today, std::move(name)) {
        LOG(INFO) << "Creating "<< info_cache_name;
    }
};
#endif