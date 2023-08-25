#include "common/utils.hpp"
#include <vector>
#include <string>
#include <absl/container/flat_hash_map.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <cstdlib>
#include <fmt/format.h>
#include <filesystem>
using json = nlohmann::json;
class ClosePrice{
public:
    std::vector<double> sh;
    std::vector<double> sz;
    std::vector<double> all;
    void init(const std::string& path){
        json hfq_output = json::parse(std::ifstream("output.json"));
        sh = [&hfq_output](){
            std::vector<double> sz_security_closeprices = hfq_output["sz_closeprice"];
            return sz_security_closeprices;
        }();
        sz = [&hfq_output](){
            std::vector<double> sh_security_closeprices = hfq_output["sh_closeprice"];
            return sh_security_closeprices;
        }();
        all = vecMerge(sz,sh);
    }
};

class SecurityId{
public:
    std::vector<std::array<char,11>> sh;
    std::vector<std::array<char,11>> sz;
    std::vector<std::array<char,11>> all;
    void init(const std::string& path){
        json hfq_output = json::parse(std::ifstream("output.json"));
        sz = [&hfq_output](){
            std::vector<std::string> sz_securitys_str = hfq_output["sz"];
            return vecConvertStrToArray<11>(sz_securitys_str);
        }();
        sh = [&hfq_output](){
            std::vector<std::string> sz_securitys_str = hfq_output["sh"];
            return vecConvertStrToArray<11>(sz_securitys_str);
        }();
        all = vecMerge(sz,sh);
    }
};

class SystemParam{
public:
    int collect_latency_ms;
    absl::CivilDay today;
    void init(){
        json config = open_json_file("config/system.json");
        //获取当天的日期，后续操作依赖于这个函数
        today = [&config](){
            absl::CivilDay today;
            /**
             * 当日日期需要直接指定，因为无法判断hfq_table中，最后一天之后多久是下一个交易日，节假日的维护不稳定
             * */
            std::string str_day = config["system"]["date"];
            if (!absl::ParseCivilTime(str_day, &today)) {
                throw std::runtime_error("Fail to get today from system.json");
            }
            return today;
        }();
        collect_latency_ms = config["system"]["collect_latency(ms)"];
    }
};

class FunctionSelect{
public:
    std::vector<int> unique_periods;
    std::unordered_map<int, std::vector<std::string>> feature_for_trade;
    std::unordered_map<int, std::vector<std::string>> feature_for_order;
    std::unordered_map<int, std::vector<std::string>> feature_for_depth;
    void init(const std::string& path){
        json config = open_json_file("config/publish.json");
        
        
    }
};

class ConfigMaker{
public:
    ClosePrice close_price;
    SecurityId security_id;
    SystemParam system_param;
public:
    void prepare();
};

void ConfigMaker::prepare(){
    if(!std::filesystem::exists("config")){
        throw std::runtime_error("config folder not find");
    }
    json config = open_json_file("config/system.json");
    //嵌入python脚本，获取当前的天数, 需要track的股票数，和前一天的收盘价closeprice
    std::string hfq_table_path = config["system"]["hfq_multi_parquet_path"];
    std::system(fmt::format("python3 script.py {}", hfq_table_path).c_str());
    close_price.init("output.json");
    security_id.init("output.json");
    std::system("rm output.json");
    
    //生成调用函数表
}

int main()
{

    return 0;
}