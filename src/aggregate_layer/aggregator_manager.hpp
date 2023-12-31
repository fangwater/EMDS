#ifndef AGGREGATOR_MANAGER_HPP
#define AGGREGATOR_MANAGER_HPP
#include <chrono>
#include "aggregator.hpp"
class AggregatorManager{
private:
    using period_update_item = std::pair<int,int64_t>;
    absl::flat_hash_map<period_update_item,int> finish_map;
    std::mutex mtx;
    bool need_dump_file;
    std::string dump_dir;
    std::shared_ptr<absl::flat_hash_map<std::array<char,11>, int>> security_id_to_row_map;
    absl::flat_hash_map<int, std::unique_ptr<std::vector<std::unique_ptr<MarketDataAggregator>>>> period_aggregators_map;
private:
    int set_security_id_to_row_map(std::shared_ptr<absl::flat_hash_map<std::array<char,11>, int>> map_sp);
    int register_aggregator(int period, std::string aggregator_name, std::string bind_to, std::vector<std::array<char, 11>> &security_ids, std::vector<std::string>& col_names);
    static std::string format_tp_to_string(int64_t time_in_micros);
public:
    int commit(PeriodResult&& period_res);
    int period_finish(int period, int64_t tp);
    explicit AggregatorManager(std::string dump_path);
    AggregatorManager():need_dump_file(false){};
    ~AggregatorManager() {
        std::cout <<"Destructing AggregatorManager..." << std::endl;
        std::cout <<"AggregatorManager destructed."<< std::endl;
    }
    int init(std::string path_to_config_folder, std::string manager_type);
};

int AggregatorManager::init(std::string path_to_config_folder, std::string manager_type){
    namespace fs = std::filesystem;
    if (!fs::exists(path_to_config_folder)) {
        throw std::runtime_error(fmt::format("Directory of config folder not find : {}",path_to_config_folder));
    }

    auto security_ids = [&path_to_config_folder](){
        std::vector<std::array<char, 11>> security_ids;
        auto security_ids_json = open_json_file(fmt::format("{}/{}",path_to_config_folder,"traced_contract.json"));
        std::vector<std::string> SH_ids_str = security_ids_json["sh"];
        std::vector<std::string> SZ_ids_str = security_ids_json["sz"];
        auto ids_str = vecMerge(SH_ids_str,SZ_ids_str);
        return vecConvertStrToArray<11>(ids_str);
    }();

    auto id_to_row_map_sp = [&security_ids](){
        auto id_to_row_map_sp = std::make_shared<absl::flat_hash_map<std::array<char,11>, int>>();
        for(int i = 0; i < security_ids.size(); i++){
            id_to_row_map_sp->insert({security_ids[i],i});
        }
        return id_to_row_map_sp;
    }();
    set_security_id_to_row_map(id_to_row_map_sp);

    auto feature_format_map = [&path_to_config_folder](){
        auto feature_format_json = open_json_file(fmt::format("{}/{}",path_to_config_folder,"feature_format.json"));
        std::unordered_map<std::string, std::vector<std::string>> feature_format_map;
        for (auto& [key, value] : feature_format_json.items()) {
            feature_format_map[key] = value.get<std::vector<std::string>>();
        }
        return feature_format_map;
    }();
    auto [unique_periods,per_period_called_functions,per_period_bind_addrs] = [&path_to_config_folder,&manager_type](){
        std::vector<int> periods;
        std::vector<std::vector<std::string>> called_functions;
        std::vector<std::vector<int>> bind_ports;

        auto period_call_json = open_json_file(fmt::format("{}/{}",path_to_config_folder,"period_call.json"));
        for (const auto& item : period_call_json[manager_type]) {
            periods.push_back(item["period"]);
            called_functions.push_back(item["called_func"].get<std::vector<std::string>>());
            bind_ports.push_back(item["port"].get<std::vector<int>>());
        }

        std::string ip = get_host_ip();
        std::vector<std::vector<std::string>> called_addrs;
        for( int i = 0; i < bind_ports.size(); i++){
            std::vector<std::string> addrs;
            for( int j = 0; j < bind_ports[i].size(); j++){
                addrs.push_back(fmt::format("tcp://{}:{}",ip,bind_ports[i][j]));
            }
            called_addrs.push_back(addrs);
        }
        return std::tuple(std::move(periods), std::move(called_functions), std::move(called_addrs));
    }();

    int aggregator_count = 0;
    for(int i = 0; i < unique_periods.size(); i++){
        for(int j = 0; j < per_period_called_functions[i].size(); j++){
            /*debug*/
            aggregator_count++;
            LOG(INFO) << fmt::format("\nCreating aggregator {}:\nperiod: {}\nfunction:{}\nbind_addr:{}\n", aggregator_count, unique_periods[i], per_period_called_functions[i][j],per_period_bind_addrs[i][j]);
            if(need_dump_file){
                std::string feature_folder_path = fmt::format("{}/{}", dump_dir, per_period_called_functions[i][j]);
                if(std::filesystem::exists(feature_folder_path)){
                    LOG(WARNING) << fmt::format("{} feature fold existed in {}", per_period_called_functions[i][j], dump_dir);
                }else{
                    std::filesystem::create_directory(feature_folder_path);
                }
            }
            register_aggregator(
                unique_periods[i],
                per_period_called_functions[i][j],
                per_period_bind_addrs[i][j],
                security_ids, 
                feature_format_map[per_period_called_functions[i][j]]);
        }
    }
    return 0;
}

AggregatorManager::AggregatorManager(std::string dump_path){
    namespace fs = std::filesystem;
    dump_dir = dump_path;
    if (fs::exists(dump_dir)) {
        LOG(WARNING) << fmt::format("Aggregators dump_dir is exist");
    } else {
        fs::create_directory(dump_dir);
        LOG(INFO) << fmt::format("Create dump_dir is success");
    } 
    need_dump_file = true;
}

int AggregatorManager::set_security_id_to_row_map(std::shared_ptr<absl::flat_hash_map<std::array<char,11>, int>> map_sp){
    security_id_to_row_map = map_sp;
    return 0;
}

int AggregatorManager::register_aggregator(int period, std::string aggregator_name, std::string bind_to, std::vector<std::array<char, 11>> &security_ids, std::vector<std::string>& col_names){
    auto iter = period_aggregators_map.find(period);
    auto aggregator_ptr = std::make_unique<MarketDataAggregator>(bind_to,aggregator_name);
    aggregator_ptr->init(security_ids.size(),col_names);
    aggregator_ptr->set_security_id_col(security_ids);
    if(iter != period_aggregators_map.end()){
        iter->second->push_back(std::move(aggregator_ptr));
    }else{
        auto aggregators_ptr = std::make_unique<std::vector<std::unique_ptr<MarketDataAggregator>>>();
        aggregators_ptr->push_back(std::move(aggregator_ptr));
        period_aggregators_map.insert({period, std::move(aggregators_ptr)});
    }
    return 0;
}

std::string AggregatorManager::format_tp_to_string(int64_t time_in_micros) {
    //需要把时间再减去，都是pandas的问题
    absl::Time time_obj = absl::FromUnixMicros(time_in_micros) - absl::Hours(8);
    const absl::TimeZone& cached_tz = absl::LocalTimeZone();
    int hours = std::stoi(absl::FormatTime("%H", time_obj, cached_tz));
    int minutes = std::stoi(absl::FormatTime("%M", time_obj, cached_tz));
    int seconds = std::stoi(absl::FormatTime("%S", time_obj, cached_tz));
    int64_t time_in_millis = absl::ToUnixMillis(time_obj);
    int milliseconds = time_in_millis % 1000;
    return fmt::format("{:02}_{:02}_{:02}_{:03}", hours, minutes, seconds, milliseconds);
}

int AggregatorManager::commit(PeriodResult&& period_res){
    auto row = security_id_to_row_map->at(period_res.security_id);
    auto& aggregators = period_aggregators_map.at(period_res.period);
    for(int i = 0; i < period_res.results.size(); i++){
        aggregators->at(i)->commit(row, period_res.results[i]);
    }
    return 0;
}

int AggregatorManager::period_finish(int period, int64_t tp){
    std::lock_guard<std::mutex> lock(mtx);
    period_update_item period_record = std::pair(period,tp);
    //Map中查看是否有存在
    auto [iter,inserted] = finish_map.try_emplace(period_record,1);
    if(inserted){
        //插入成功，代表这个period_tp从未被提交过
    }else{
        iter->second++;
        if(iter->second > 2){
            LOG(WARNING) << "Consecutive first calls to period_finish without a second call!";
        }else{
            LOG(INFO) << fmt::format("Start publish for period_{} ......",period);
        }
        //第二次插入这个key, 正确
        finish_map.erase(iter);
        auto& aggregators = period_aggregators_map.at(period);
        // int64_t publish_cost = 0;
        // int64_t dump_cost = 0;
        // for(int i = 0; i < aggregators->size(); i++){
        //     auto& aggregator = aggregators->at(i);
        //     aggregator->set_time_col(tp);

        //     auto start_publish = std::chrono::system_clock::now();
        //     aggregator->publish();
        //     auto end_publish = std::chrono::system_clock::now();
        //     publish_cost += std::chrono::duration_cast<std::chrono::milliseconds>(end_publish - start_publish).count();

        //     if(need_dump_file){
        //         /*path: dump_dir/function_name/period_i/duration*/
        //         /* 2023_08_12/candle_stick/period_{}/09_31_03_000 */
        //         std::string folder_dir = fmt::format("{}/{}/period_{}",dump_dir,aggregator->get_name(),period);
        //         if(!std::filesystem::exists(folder_dir)){
        //             std::filesystem::create_directories(folder_dir);
        //         }
        //         std::string path = fmt::format("{}/{}.parquet",folder_dir,format_tp_to_string(tp));
        //         aggregator->dump(path);
        //         // (void)std::async(std::launch::async, [&aggregator,dump_path = std::move(path)](){
        //         //     aggregator->dump(dump_path);
        //         // });
        //         auto end_dump = std::chrono::system_clock::now();
        //         dump_cost += std::chrono::duration_cast<std::chrono::milliseconds>(end_dump - end_publish).count();
        //     }
        // }
        std::atomic<int64_t> max_publish_delay = 0;
        std::atomic<int64_t> max_dump_delay = 0;
        std::vector<std::jthread> workers;
        auto start_publish = std::chrono::system_clock::now();
        for(int i = 0; i < aggregators->size(); i++){
            workers.emplace_back([&, i](std::stop_token stoken) {
                auto& aggregator = aggregators->at(i);
                aggregator->set_time_col(tp);
                aggregator->publish();
                auto end_publish = std::chrono::system_clock::now();
                int64_t current_publish_delay = std::chrono::duration_cast<std::chrono::milliseconds>(end_publish - start_publish).count();
                int64_t prev_publish_delay = max_publish_delay.load(std::memory_order_relaxed);
                while (current_publish_delay > prev_publish_delay &&
                    !max_publish_delay.compare_exchange_strong(prev_publish_delay, current_publish_delay, std::memory_order_relaxed));

                if(need_dump_file){
                    /*path: dump_dir/function_name/period_i/duration*/
                    /* 2023_08_12/candle_stick/period_{}/09_31_03_000 */
                    std::string folder_dir = fmt::format("{}/{}/period_{}", dump_dir, aggregator->get_name(), period);
                    if(!std::filesystem::exists(folder_dir)){
                        std::filesystem::create_directories(folder_dir);
                    }
                    std::string path = fmt::format("{}/{}.parquet", folder_dir, format_tp_to_string(tp));
                    aggregator->dump(path);
                    auto end_dump = std::chrono::system_clock::now();
                    int64_t current_dump_delay = std::chrono::duration_cast<std::chrono::milliseconds>(end_dump - end_publish).count();
                    int64_t prev_dump_delay = max_dump_delay.load(std::memory_order_relaxed);
                    while (current_dump_delay > prev_dump_delay &&
                        !max_dump_delay.compare_exchange_strong(prev_dump_delay, current_dump_delay, std::memory_order_relaxed));
                }
            });
        }
        // Wait for all threads to complete
        for(auto& worker : workers) {
            worker.join();
        }
        LOG(INFO) << fmt::format("Finsh publish for {} features period_{}", aggregators->size(), period);

        if (need_dump_file) {
            LOG(INFO) << fmt::format("Max publish delay: {}ms, Max persistence delay: {}ms", max_publish_delay.load(), max_dump_delay.load());
        }
    }

    return 0;
}
#endif