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
#endif //INFO_HPP
