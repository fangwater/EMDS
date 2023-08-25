#ifndef AGGREGATOR_MANAGER_HPP
#define AGGREGATOR_MANAGER_HPP
#include "aggregator.hpp"
class AggregatorManager{
private:
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
    explicit AggregatorManager(std::string dump_dir);
    AggregatorManager():need_dump_file(false){};
    int init(std::string path_to_config_folder, std::string manager_type);
};

int AggregatorManager::init(std::string path_to_config_folder, std::string manager_type){
    namespace fs = std::filesystem;
    if (!fs::exists(path_to_config_folder)) {
        throw std::runtime_error(fmt::format("Directory of config folder not find : {}",path_to_config_folder));
    }

    auto security_ids = [&path_to_config_folder](){
        std::vector<std::array<char, 11>> security_ids;
        auto security_ids_json = open_json_file(fmt::format("{}/{}",path_to_config_folder,"security_ids.json"));
        for (const auto& item : security_ids_json["total"]) {
            std::array<char, 11> arr;
            std::string strItem = item;
            if (strItem.length() == 11) { // Ensure the string is exactly of length 11
                std::copy(strItem.begin(), strItem.end(), arr.begin());
                security_ids.push_back(arr);
            } else {
                throw std::runtime_error("security_id has format error");
            }
        }
        return security_ids;
    }();

    auto id_to_row_map_sp = [&security_ids](){
        auto id_to_row_map_sp = std::make_shared<absl::flat_hash_map<std::array<char,11>, int>>();
        for(int i = 0; i < security_ids.size(); i++){
            id_to_row_map_sp->insert({security_ids[i],i});
        }
        return id_to_row_map_sp;
    }();
    set_security_id_to_row_map(id_to_row_map_sp);

    auto feature_format_map = [&path_to_config_folder,this](){
        auto feature_format_json = open_json_file(fmt::format("{}/{}",path_to_config_folder,"feature_format.json"));
        std::unordered_map<std::string, std::vector<std::string>> feature_format_map;
        for (auto& [key, value] : feature_format_json.items()) {
            feature_format_map[key] = value.get<std::vector<std::string>>();
        }
        return feature_format_map;
    }();
    auto [unique_periods,per_period_called_functions,per_period_bind_addrs] = [&path_to_config_folder,&manager_type](){
        std::vector<int> unique_periods;
        std::vector<std::vector<std::string>> called_functions;
        std::vector<std::vector<int>> bind_ports;

        auto period_call_json = open_json_file(fmt::format("{}/{}",path_to_config_folder,"period_call.json"));
        for (const auto& item : period_call_json[manager_type]) {
            unique_periods.push_back(item["period"]);
            called_functions.push_back(item["called_func"].get<std::vector<std::string>>());
            bind_ports.push_back(item["port"].get<std::vector<int>>());
        }

        std::string ip = period_call_json["ip"];
        std::vector<std::vector<std::string>> called_addrs;
        for( int i = 0; i < bind_ports.size(); i++){
            std::vector<std::string> addrs;
            for( int j = 0; j < bind_ports[i].size(); j++){
                addrs.push_back(fmt::format("tcp://{}:{}",ip,bind_ports[i][j]));
            }
            called_addrs.push_back(addrs);
        }
        return std::tuple(std::move(unique_periods), std::move(called_functions), std::move(called_addrs));
    }();

    int aggregator_count = 0;
    for(int i = 0; i < unique_periods.size(); i++){
        for(int j = 0; j < per_period_called_functions[i].size(); j++){
            /*debug*/
            aggregator_count++;
            DLOG(INFO) << fmt::format("\nCreating aggregator {}:\nperiod: {}\nfunction:{}\nbind_addr:{}\n", aggregator_count, unique_periods[i], per_period_called_functions[i][j],per_period_bind_addrs[i][j]);
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
    absl::Time time_obj = absl::FromUnixMicros(time_in_micros);
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
};

int AggregatorManager::period_finish(int period, int64_t tp){
    auto& aggregators = period_aggregators_map.at(period);
    for(int i = 0; i < aggregators->size(); i++){
        auto& aggregator = aggregators->at(i);
        aggregator->set_time_col(tp);
        aggregator->publish();
        if(need_dump_file){
            /*path: dump_dir/function_name/period_i/duration*/
            /* 2023_08_12/candle_stick/period_{}/09_31_03_000 */
            std::string folder_dir = fmt::format("{}/{}/period_{}",dump_dir,aggregator->get_name(),period);
            if(!std::filesystem::exists(folder_dir)){
                std::filesystem::create_directories(folder_dir);
            }
            std::string path = fmt::format("{}/{}.parquet",folder_dir,format_tp_to_string(tp));
            aggregator->dump(path);
            // (void)std::async(std::launch::async, [&aggregator,dump_path = std::move(path)](){
            //     aggregator->dump(dump_path);
            // });
        }  
    }
    return 0;
}
#endif