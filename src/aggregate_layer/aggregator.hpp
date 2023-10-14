#ifndef AGGREGATOR_HPP
#define AGGREGATOR_HPP
#include <zmq.hpp>
#include <absl/time/time.h>
#include <absl/container/flat_hash_map.h>
#include <absl/log/absl_log.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include "parquet_io.hpp"
#include "period_result.hpp"
#include "utils.hpp"
using json = nlohmann::json;
class ZmqSender{
private:
    zmq::context_t context;
    zmq::socket_t sender;
public:
    explicit ZmqSender(const std::string& bind_to): context(1),sender(context, zmq::socket_type::pub)
    {
        sender.set(zmq::sockopt::sndbuf, 1024 * 1024 * 16);
        sender.set(zmq::sockopt::sndhwm, 0);
        sender.bind(bind_to);
    };
    int forward(std::shared_ptr<arrow::Buffer> buffer);
    ~ZmqSender(){
        sender.close();
        context.close();
    }
};

int ZmqSender::forward(std::shared_ptr<arrow::Buffer> buffer)
{
    //b aggregator.hpp:38
    zmq::message_t non_blocking_msg(buffer->data(), buffer->size());
    bool sent = false;
    int retry_count = 0;
    const int max_retries = 5;
    while (!sent && retry_count < max_retries) {
        try {
            sender.send(non_blocking_msg, zmq::send_flags::dontwait);
            sent = true;
        } catch (const zmq::error_t& e) {
            if (e.num() == EAGAIN) {
                retry_count++;
                // sleep for 10 milliseconds before retry
                LOG(WARNING) << fmt::format("Zmq fail to forward table, retry {} time",retry_count);
                std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
            } else {
                throw;
            }
        }
    }
    if (!sent) {
        return -1;  // Indicate that the message was not sent after max_retries
    }
    return 1;  // Indicate that the message was successfully sent
}


class Table{
public:
    /*data of table*/
    std::shared_ptr<TableDescriptor> table_descriptor;
public:
    /*shape of table*/
    int rows,cols;
public:
    int construct(int64_t n, std::vector<std::string>& col_types, 
        std::vector<std::string>& col_names, std::vector<uint32_t>& ele_size);
    
    template <typename T, std::size_t N>
    requires std::is_standard_layout_v<T>
    int set_col_by_value(const std::array<T, N>& arr, std::string col_name);

    template<typename T>
    requires std::is_trivially_copyable_v<T>
    int set_col_by_value(T val, std::string col_name);

    template <std::size_t N>
    int set_col_by_arrays(const std::vector<std::array<char, N>>& arrs, std::string col_name);

    template <typename T>
    T& at(int row, int col){
        return reinterpret_cast<T*>(table_descriptor->bufs[col].first)[row];
    }

    void set_nan(int row, int col){
        set_bit(table_descriptor->is_Nulls_buffer[col], row);
    }

    std::shared_ptr<arrow::Buffer> serialize_to_arrow(){
        return serialize_to_arrow_ipc(table_descriptor);
    }

    void persist_to_parquet_format(const std::string& path){
        arrow::Status st = persist_to_parquet(path,table_descriptor);
    }

};

template <std::size_t N>
int Table::set_col_by_arrays(const std::vector<std::array<char, N>>& arrs, std::string col_name){
    int index = [this,&col_name]() mutable {
        for(int i = 0; i < this->table_descriptor->col_names.size(); i++){
            if(this->table_descriptor->col_names[i] == col_name){
                return i;
            }
        }
        throw std::runtime_error("Column name not found: " + col_name);
    }();
    auto data_ptr = reinterpret_cast<char*>(this->table_descriptor->bufs[index].first);
    auto ele_count = arrs.size();
    for(int j = 0; j < ele_count; j++){
        std::memcpy(data_ptr + j*N, arrs[j].data(), N);
    }
    return 0;
};

template <typename T, std::size_t N>
requires std::is_standard_layout_v<T>
int Table::set_col_by_value(const std::array<T, N>& arr, std::string col_name){
    int index = [this,&col_name]() mutable{
        for(int i = 0; i < this->table_descriptor->col_names.size(); i++){
            if(this->table_descriptor->col_names[i] == col_name){
                return i;
            }
        }
        throw std::runtime_error("Column name not found: " + col_name);
    }();
    auto data_ptr = reinterpret_cast<T*>(this->table_descriptor->bufs[index].first);
    auto ele_count = this->table_descriptor->bufs[index].second / N;
    for(int j = 0; j < ele_count; j++){
        std::memcpy(data_ptr + j * sizeof(T) * N, arr.data(), sizeof(T) * N);
    }
    return 0;
};

template<typename T>
requires std::is_trivially_copyable_v<T>
int Table::set_col_by_value(T val, std::string col_name){
    int index = [this, &col_name]() mutable {
        for(int i = 0; i < this->table_descriptor->col_names.size(); i++){
            if(this->table_descriptor->col_names[i] == col_name){
                return i;
            }
        }
        throw std::runtime_error("Column name not found: " + col_name);
    }();
    if(index >= 0){  
        if(table_descriptor->col_types[index] == "_uint64"){
            auto data_ptr = reinterpret_cast<uint64_t*>(table_descriptor->bufs[index].first);
            auto ele_count = table_descriptor->bufs[index].second / sizeof(uint64_t);
            std::fill(data_ptr, data_ptr + ele_count, static_cast<uint64_t>(val));
        }else if(table_descriptor->col_types[index] == "_int64" || table_descriptor->col_types[index] == "_timestramp"){
            auto data_ptr = reinterpret_cast<int64_t*>(table_descriptor->bufs[index].first);
            auto ele_count = table_descriptor->bufs[index].second / sizeof(int64_t);
            std::fill(data_ptr, data_ptr + ele_count, static_cast<int64_t>(val));
        }else if(table_descriptor->col_types[index] == "_double"){
            auto data_ptr = reinterpret_cast<double*>(table_descriptor->bufs[index].first);
            auto ele_count = table_descriptor->bufs[index].second / sizeof(double);
            std::fill(data_ptr, data_ptr + ele_count, static_cast<double>(val));
        }else{
            throw std::runtime_error("Unsupport type for table");
        }
        return index;
    }
    throw std::runtime_error("Index < 0 for column: " + col_name);
    return -1;
}


int Table::construct(int64_t n, std::vector<std::string>& col_types,
                 std::vector<std::string>& col_names, std::vector<uint32_t>& ele_size){
        table_descriptor = std::make_shared<TableDescriptor>();
        table_descriptor->col_names = col_names;
        table_descriptor->col_types = col_types;
        table_descriptor->ele_size = ele_size;
        rows = n;
        cols = col_names.size();
        table_descriptor->bufs = [&n, &ele_size](){
            std::vector<buf> bufs;
            for(auto& size : ele_size){
                size_t length = n * size;
                auto* membuf = new uint8_t[length];
                bufs.emplace_back(membuf,length);
            }
            return bufs;
        }();
        table_descriptor->is_Nulls_buffer = [&n, &col_types](){
            std::vector<uint8_t*> is_Nulls_buffer;
            for(auto& col_type : col_types){
                if(col_type == "_fixed_str" || col_type == "_uint64"){
                    is_Nulls_buffer.push_back(nullptr);
                }else{
                    //目前都是8, 默认有效
                    auto* bitmap_ptr = new uint8_t[static_cast<int>(std::ceil(static_cast<double>(n)/8.0))]{};
                    set_bits(bitmap_ptr, n);
                    is_Nulls_buffer.push_back(bitmap_ptr);
                }
            }
            return is_Nulls_buffer;//RVO
        }();
        return 0;
};

class MarketDataAggregator{
private:
    Table table;
    ZmqSender sender;
    int committed;
    std::string name;
public:
    template <std::size_t N>
    int set_security_id_col(std::vector<std::array<char,N>>& security_ids);
    int set_time_col(int64_t ms_tp);
    int commit(int row, std::vector<double>& feature_res);
    int publish();
    [[nodiscard]] const std::string& get_name() const { return name; }
    void dump(const std::string& path);
    explicit MarketDataAggregator(const std::string& bind_to, const std::string& function_name)
        :sender(bind_to),committed(0),name(function_name){};
    int init(int64_t n, std::vector<std::string>& feature_col_names){
        /*force to security_id + datetime + double[] format*/
        std::vector<std::string> col_types = {"_fixed_str", "_timestramp"};
        std::vector<std::string> col_names = {"securityid", "datetime"};
        std::vector<uint32_t> ele_size = {11,8};
        for(auto& feature_col_name : feature_col_names){
            col_types.emplace_back("_double");
            ele_size.push_back(8);
            col_names.push_back(feature_col_name);
        }
        table.construct(n, col_types,col_names,ele_size);
        return 0;
    }
};

int MarketDataAggregator::publish(){
    if(committed > table.rows){
        LOG(WARNING) << fmt::format("{} committed {}, but only need {}",name,committed,table.rows);
    }
    committed = 0;
    auto arrow_buffer_sp = table.serialize_to_arrow();
    return sender.forward(arrow_buffer_sp);
}

void MarketDataAggregator::dump(const std::string& path){
    table.persist_to_parquet_format(path);
}

template <std::size_t N>
int MarketDataAggregator::set_security_id_col(std::vector<std::array<char,N>>& security_ids){
    return table.set_col_by_arrays(security_ids,"securityid");
};

int MarketDataAggregator::set_time_col(int64_t ms_tp){
    return table.set_col_by_value(ms_tp,"datetime");
}

int MarketDataAggregator::commit(int row, std::vector<double>& feature_res){
    for(int i = 0; i < feature_res.size(); i++){
        if(!std::isnan(feature_res[i])){
            // template <typename T>
            // T& at(int row, int col){
            //     return reinterpret_cast<T*>(table_descriptor->bufs[col].first)[row];
            // }
            table.at<double>(row,i+2) = feature_res[i];
        }else{
            table.set_nan(row,i+2);
        }
    }
    committed++;
    return 0;
};


#endif
