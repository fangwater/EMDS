#ifndef TRADE_DEBUG_HPP
#define TRADE_DEBUG_HPP
#include <tuple>
#include <array>
#include <cstdint>
#include <memory>
#include <vector>
#include <xsimd/xsimd.hpp>
#include "../common/info.hpp"
#include "feature_callback.hpp"

class TradeDebug : public FeatureCallBack<TradeInfo>{
private:
    std::vector<int64_t> term_count;
public:
    explicit TradeDebug(int index_count):term_count(index_count){}
    std::vector<double> calculate(const PackedInfoSp<TradeInfo>& tick_buffer_sp,int security_id_index) override;
};

std::vector<double> TradeDebug::calculate(const PackedInfoSp<TradeInfo>& tick_buffer_sp,int security_id_index){
    if(tick_buffer_sp->size() == 0){
        return std::vector<double>{0,0,0};
    }
    term_count[security_id_index] += 1;
    double amount = 0;
    double count = 0;
    for(auto& trade : *tick_buffer_sp){
        amount += trade->TradPrice * trade->TradVolume;
        count += 1.0;
    }
    return std::vector<double>{amount,count,static_cast<double>(term_count[security_id_index])};
}

#endif