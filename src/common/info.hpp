#ifndef INFO_HPP
#define INFO_HPP
#include <absl/time/time.h>
#include <array>
#include <memory>

struct DepthInfo{
    absl::Duration dur;
    std::array<char,11> SecurityID;
    double LastPrice;
    double TradNumber;
    double TradVolume;
    double Turnover;
    double TotalBidVol;
    double WAvgBidPri;
    double TotalAskVol;
    double WAvgAskPri;
    double AskPrice_n[10];
    double AskVolume_n[10];
    double BidPrice_n[10];
    double BidVolume_n[10];
}__attribute__ ((aligned(8)));

struct TradeInfo{
    absl::Duration dur;
    std::array<char,11> SecurityID;
    int32_t B_or_S;
    absl::Time TradTime;
    double TradPrice;
    double TradVolume;
}__attribute__ ((aligned(8)));

struct OrderInfo{
    absl::Duration dur;
    std::array<char,11> SecurityID;
    int32_t B_or_S;
    absl::Time TradTime;
    double TradPrice;
    double TradVolume;
}__attribute__ ((aligned(8)));

template<typename T>
using PackedInfoSp = std::shared_ptr<std::vector<std::shared_ptr<T>>>;

enum class EXCHANGE{
    SZ,
    SH
};

template<typename T, EXCHANGE EX>
constexpr std::string_view str_type_ex() {
    if constexpr (std::is_same_v<T, TradeInfo>) {
        if constexpr (EX == EXCHANGE::SH) {
            return "Trade : SH";
        } else if constexpr (EX == EXCHANGE::SZ) {
            return "Trade : SZ";
        }
    } else if constexpr (std::is_same_v<T, DepthInfo>) {
        if constexpr (EX == EXCHANGE::SH) {
            return "Depth : SH";
        } else if constexpr (EX == EXCHANGE::SZ) {
            return "Depth : SZ";
        }
    } else if constexpr (std::is_same_v<T, OrderInfo>) {
        if constexpr (EX == EXCHANGE::SH) {
            return "Order : SH";
        } else if constexpr (EX == EXCHANGE::SZ) {
            return "Order : SZ";
        }
    } else {
        return "Error Format";
    }
}

template<typename T, EXCHANGE EX>
constexpr std::string_view str_type_info() {
    if constexpr (std::is_same_v<T, TradeInfo>) {
        return "Trade";
    } else if constexpr (std::is_same_v<T, DepthInfo>) {
        return "Depth";
    } else if constexpr (std::is_same_v<T, OrderInfo>) {
        return "Order";
    } else {
        return "Error Format";
    }
}
#endif //INFO_HPP
