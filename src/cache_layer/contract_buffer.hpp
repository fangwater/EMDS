#ifndef CONTRACT_HPP
#define CONTRACT_HPP
#include <absl/container/flat_hash_map.h>
#include <mutex>
#include <folly/ProducerConsumerQueue.h>
#include <memory>
#include <vector>
#include <absl/log/log.h>

template<typename T>
class ContractBuffer{
public:
    mutable std::mutex mtx;
    std::unique_ptr<folly::ProducerConsumerQueue<std::shared_ptr<T>>> buffer_ptr;
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
            LOG(FATAL) << "Queue is out of size";
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

template<typename T>
class ContractBufferMap{
public:
    absl::flat_hash_map<std::array<char,11>, ContractBuffer<T>> securiy_id_to_contract_buffer_map;
public:
    ContractBufferMap(std::vector<std::array<char,11>>& security_ids, std::size_t buffer_size){
        securiy_id_to_contract_buffer_map.reserve(security_ids.size());
        for(auto& security_id : security_ids){
            securiy_id_to_contract_buffer_map.insert( {security_id, ContractBuffer<T>(buffer_size)} );
        }
    }
    bool insert(std::shared_ptr<T> info){
        auto iter = securiy_id_to_contract_buffer_map.find(info->SecurityID);
        if(iter == securiy_id_to_contract_buffer_map.end()){
            LOG(INFO) << std::string(info->SecurityID.begin(), info->SecurityID.end());
            LOG(WARNING) << "this security_id is not included in the securiy_id_to_contract_buffer_map";
            return false;
        }else{
            iter->second.append_info(info);
            return true;
        }
    }
};

#endif
