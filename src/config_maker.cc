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
class SecurityId{
public:
    std::vector<std::array<char,11>> sh;
    std::vector<std::array<char,11>> sz;
    std::vector<std::array<char,11>> all;
    void init(const std::string& path){
        json hfq_output = json::parse(std::ifstream("traced_contract.json"));
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



class ConfigMaker{
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
    std::system("rm output.json");
    
    //生成调用函数表
}

int main()
{

    return 0;
}