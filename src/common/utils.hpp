#ifndef UTILS_HPP
#define UTILS_HPP
#include <vector>
#include <array>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <queue>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <fmt/format.h>
#include <iostream>
#include <fstream>
#include <numeric>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "info.hpp"

using json = nlohmann::json;

inline void CHECK_RETURN_VALUE(bool res, std::string error_msg){
    if(!res){
        std::cerr << error_msg << std::endl;
    }
    return;
}

std::string get_host_ip(){
    char hostbuffer[256];
    gethostname(hostbuffer, sizeof(hostbuffer));
    struct hostent *host_entry = gethostbyname(hostbuffer);
    if (host_entry == NULL) {
        throw std::runtime_error(fmt::format("Failed to get host entry for {}",hostbuffer));
    }
    char *IPbuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0]));
    return std::string(IPbuffer);
}


inline std::string absl_time_to_str(absl::Time now){
    std::string time_str = absl::FormatTime("%m-%d %H:%M:%S.%f", now, absl::LocalTimeZone());
    // Truncate the last three characters (microseconds -> milliseconds)
    time_str = time_str.substr(0, time_str.size() - 3);
    return time_str;
}

template<typename T,EXCHANGE EX>
void set_trade_suffix(std::array<char, 11>& arr) {
    if constexpr (std::is_same_v<T,TradeInfo>){
        if constexpr (EX == EXCHANGE::SZ){
            const char* suffix = ".XSHE";
            std::memcpy(&arr[arr.size() - 5], suffix, 5);
        }else {
            const char* suffix = ".XSHG";
            std::memcpy(&arr[arr.size() - 5], suffix, 5);
        }
    }
}


json open_json_file(std::string path_to_json){
    std::ifstream config_file(path_to_json);
    if (!config_file.is_open()) {
         throw std::runtime_error(fmt::format("Failed to open file: : {}", path_to_json));
    }
    json config_json;
    config_file >> config_json;
    return config_json;
}
/**
 * 优先级队列实现多路并归，用于整合多个有序周期为一个，并原vec在合并后的index
*/
std::pair<std::vector<int>, std::vector<std::vector<int>>> K_way_ordered_vec_merge(const std::vector<std::vector<int>>& vectors) {
    struct Element {
        int val;
        int vecIndex;
        int eleIndex;

        Element(int v, int vi, int ei) : val(v), vecIndex(vi), eleIndex(ei) {}
        bool operator>(const Element& e) const { return val > e.val; }
    };

    std::priority_queue<Element, std::vector<Element>, std::greater<>> pq;
    std::unordered_map<int, int> valueToIndex;
    std::vector<int> merged;
    std::vector<std::vector<int>> indices(vectors.size());

    for (int i = 0; i < vectors.size(); ++i) {
        if (!vectors[i].empty()) {
            pq.push(Element(vectors[i][0], i, 0));
        }
    }

    while (!pq.empty()) {
        auto top = pq.top();
        pq.pop();

        if (!valueToIndex.count(top.val)) {
            valueToIndex[top.val] = merged.size();
            merged.push_back(top.val);
        }

        indices[top.vecIndex].push_back(valueToIndex[top.val]);

        if (top.eleIndex + 1 < vectors[top.vecIndex].size()) {
            pq.push(Element(vectors[top.vecIndex][top.eleIndex + 1], top.vecIndex, top.eleIndex + 1));
        }
    }

    return {merged, indices};
}

template <typename T>
std::vector<T> vecMerge(const std::vector<T>& vec1, const std::vector<T>& vec2) {
    std::vector<T> result;
    result.reserve(vec1.size() + vec2.size()); // 预留足够的空间
    result.insert(result.end(), vec1.begin(), vec1.end()); // 插入 vec1 的所有元素
    result.insert(result.end(), vec2.begin(), vec2.end()); // 插入 vec2 的所有元素
    return result;
}

template<std::size_t N>
std::vector<std::array<char, N>> vecConvertStrToArray(const std::vector<std::string>& input) {
    std::vector<std::array<char, N>> output;

    for (const auto& str : input) {
        std::array<char, N> arr{};
        std::copy_n(str.begin(), std::min(str.size(), arr.size()), arr.begin());
        output.push_back(arr);
    }

    return output;
}

template <std::size_t N>
std::array<char, N> stringViewToArray(std::string_view str) {
    if (str.size() > N) {
        throw std::invalid_argument("String is too large to fit in array");
    }
    std::array<char, N> arr{};
    std::copy(str.begin(), str.end(), arr.begin());
    return arr;
}

template <std::size_t N>
std::string char_array_to_string(const std::array<char, N>& char_array) {
    return std::string(char_array.begin(), char_array.end());
}

void set_bit(uint8_t* bitmap_ptr, std::size_t k) {
    std::size_t index = k / 8;
    std::size_t offset = k % 8;
    bitmap_ptr[index] |= (1 << offset);
}

void set_bits(uint8_t* ptr, size_t k) {
    std::size_t index = k / 8;
    std::size_t offset = k % 8;
    for (size_t i = 0; i < index; ++i) {
        *ptr |= 0xFF;
        ++ptr;
    }
    uint8_t mask = (1 << offset) - 1;
    *ptr |= mask;
}

void clear_bit(uint8_t* bitmap_ptr, std::size_t k) {
    std::size_t index = k / 8;
    std::size_t offset = k % 8;
    bitmap_ptr[index] &= ~(1 << offset);
}

void clear_bits(uint8_t* ptr, size_t k) {
    std::size_t index = k / 8;
    std::size_t offset = k % 8;
    for (size_t i = 0; i < index; ++i) {
        *ptr = 0x00;
        ++ptr;
    }
    uint8_t mask = (1 << offset) - 1;
    *ptr &= ~mask;
}

bool is_dual_NUMA() {
    const std::string basePath = "/sys/devices/system/node/";
    int nodeCount = 0;

    // Check if the directory exists before trying to iterate over it
    if (!std::filesystem::exists(basePath)) {
        return false;
    }

    for (const auto &entry : std::filesystem::directory_iterator(basePath)) {
        if (entry.is_directory() && entry.path().filename().string().find("node") != std::string::npos) {
            nodeCount++;
        }
    }

    return nodeCount > 1;
}


#endif //UTILS_HPP
