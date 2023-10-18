#ifndef CONTRACT_HPP
#define CONTRACT_HPP
#include <absl/container/flat_hash_map.h>
#include <mutex>
#include <folly/ProducerConsumerQueue.h>
#include <memory>
#include <vector>
#include <absl/log/log.h>
#include "../common/info.hpp"
#include "../common/utils.hpp"
#include "../common/config_type.hpp"
#include "../logger/async_logger.hpp"

template<typename T>
class ContractBuffer{
public:
    mutable std::mutex mtx;
    std::unique_ptr<folly::ProducerConsumerQueue<std::shared_ptr<T>>> buffer_ptr;
    ContractBuffer() = delete;
    explicit ContractBuffer(std::size_t queue_size){
        buffer_ptr = std::make_unique< folly::ProducerConsumerQueue<std::shared_ptr<T>> >( queue_size );
    }
    ContractBuffer(ContractBuffer&& other) noexcept {
        std::lock_guard<std::mutex> lock(other.mtx);
        buffer_ptr = std::move(other.buffer_ptr);
    }

    //写可能并发,需要加锁
    bool append_info(std::shared_ptr<T> info_sp){
        std::lock_guard<std::mutex> lock(mtx);
        if( !buffer_ptr->write(info_sp) ){
            throw std::runtime_error("Queue is out of size");
        }
        return true;
    };

    //读不会有多个线程,无需加锁
    std::shared_ptr<std::vector<std::shared_ptr<T>>> flush_by_time_threshold(absl::Time time_threshold){
        std::shared_ptr<std::vector<std::shared_ptr<T>>> flushed_infos = std::make_shared<std::vector<std::shared_ptr<T>>>();
        std::shared_ptr<T> ticker_info_sp_before = nullptr;
        while(!buffer_ptr->isEmpty()){
            //getting all tick_info_ptr of this SecurityID
            if( ( *(buffer_ptr->frontPtr()) )->TradTime <= time_threshold){
                // only flush the ticker_info not in the same min of lastest ticker_info_sp
                buffer_ptr->read(ticker_info_sp_before);
                flushed_infos->push_back(ticker_info_sp_before);
            }else{
                //ok, all tick_info in buffer before ticker_info_sp->TradTime has been flushed;
                break;
            }
        }
        return flushed_infos;
    }
};

template<typename T, EXCHANGE EX>
class ContractBufferMap{
public:
    absl::flat_hash_map<std::array<char,11>, ContractBuffer<T>> security_id_to_contract_buffer_map;
public:
    ContractBufferMap() = default;
    void init(){
        std::vector<std::array<char,11>> security_ids = [](){
            SecurityId security_id_config;
            security_id_config.init();
            if constexpr (EX == EXCHANGE::SH){
                return security_id_config.sh;
            }else if constexpr (EX == EXCHANGE::SZ){
                return security_id_config.sz;
            }else{
                throw std::runtime_error("Invalid exchange type");
            }
        }();
        std::size_t contract_buffer_size = [](){
            SystemParam system_param;
            system_param.init();
            return system_param.contract_buffer_size;
        }();
        security_id_to_contract_buffer_map.reserve(security_ids.size());
        for(auto& security_id : security_ids){
            security_id_to_contract_buffer_map.insert( {security_id, ContractBuffer<T>(contract_buffer_size)} );
        }
    }
    bool insert(std::shared_ptr<T> info){
        auto iter = security_id_to_contract_buffer_map.find(info->SecurityID);
        if(iter != security_id_to_contract_buffer_map.end()){
            iter->second.append_info(info);
            return true;
        }else{
            //不应该要求跟踪全部股票，不存在的忽略即可
            //throw std::runtime_error("security_id is not included in the securiy_id_to_contract_buffer_map");
            LOG(INFO) << "security_id is not included in the securiy_id_to_contract_buffer_map";
            return false;
        }
    }
};

#endif
