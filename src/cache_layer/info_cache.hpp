#ifndef INFO_CACHE_HPP
#define INFO_CACHE_HPP
#include <folly/MPMCPipeline.h>
#include <folly/ProducerConsumerQueue.h>
#include <memory>
#include <absl/time/time.h>
#include <thread>
#include <absl/log/log.h>
#include <fmt/format.h>
#include <memory>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include "../common/time_utils.hpp"
#include "../common/utils.hpp"
#include "../common/info.hpp"
#include "contract_buffer.hpp"

template<typename T>
class InfoCache : public std::enable_shared_from_this<InfoCache<T>>{
protected:
    std::string info_cache_name;
public:
    std::unique_ptr<folly::MPMCPipeline<std::shared_ptr<T>>> info_queue_ptr;
    std::shared_ptr<ContractBufferMap<T>> security_id_to_buffer_map_sp;
    absl::Time today_start;
    std::vector<std::jthread> buffer_submitter;
public:
    InfoCache(std::size_t queue_size, absl::CivilDay today, std::string&& name){
        info_queue_ptr = std::make_unique<folly::MPMCPipeline<std::shared_ptr<T>>>(queue_size);
        today_start = absl::FromCivil(today, sh_tz.tz);
        info_cache_name = name;
    }
    //put Info to MPSC queue after parser
    void put_info(std::shared_ptr<T> info_sp){
        if( !info_queue_ptr->write(info_sp)){
            LOG(FATAL) << "Info queue size not enough";
        }
    }
    //load Info from MPSC queue
    std::shared_ptr<T> load_info(){
        //check if the queue is empty
        std::shared_ptr<T> info_ptr;
        if( !info_queue_ptr->read(info_ptr) ){
            return nullptr;
        }
        return info_ptr;
    }

    void sumbit_to_contract_buffer(std::stop_token stoken){
        while(!stoken.stop_requested()){
            std::shared_ptr<T> info_ptr = load_info();
            if(info_ptr != nullptr){
                if(!security_id_to_buffer_map_sp->insert(info_ptr)){
                    LOG(WARNING) << fmt::format("Insert: {} failed",info_ptr->SecurityID);
                }
            }else{
                std::this_thread::yield();
            }

        }
    }
    void init_submit_threadpool(int num_threads){
        auto self = this->shared_from_this();
        for(int i = 0; i < num_threads; i++){
            buffer_submitter.emplace_back(
                    [self](std::stop_token stoken){
                        self->sumbit_to_contract_buffer(stoken);
                    });
        }
        LOG(INFO) << "Success create thread pool for info_cache " << info_cache_name;
    }
    virtual int init_contract_buffer_map(std::vector<std::array<char,11>>& security_ids, std::size_t buffer_size) = 0;
    virtual void init_message_parser(std::string&& parser_type) = 0;
    ~InfoCache(){
        for(auto& thread : buffer_submitter){
            thread.request_stop();
        }
    }
};
#endif //INFO_CACHE_HPP
