#ifndef CALLBACK_FACTORY_HPP
#define CALLBACK_FACTORY_HPP
#include "config_type.hpp"
#include "../compute_lib/feature_callback.hpp"
#include "../collect_layer/collector.hpp"
#include "../compute_lib/candle_stick.hpp"

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

class CallBackObjFactory{
private:
    static CallBackObjPtr<TradeInfo> create_candle_stick_callback(std::vector<double>& closeprices);
    static CallBackObjPtr<TradeInfo> create_debug_trade_function(int index_count);
public:
    template<EXCHANGE EX>
    static CallBackObjPtr<TradeInfo> create_trade_function_callback(const std::string& name);
};

CallBackObjPtr<TradeInfo> CallBackObjFactory::create_debug_trade_function(int index_count){
    auto trade_debug_callback_ptr = std::make_unique<TradeDebug>(index_count);
    CallBackObjPtr<TradeInfo> callback_obj_ptr = std::move(trade_debug_callback_ptr);
    return callback_obj_ptr;
}

CallBackObjPtr<TradeInfo> CallBackObjFactory::create_candle_stick_callback(std::vector<double> &closeprices) {
    auto candle_stick_callback_ptr = std::make_unique<CandleStick>(closeprices);
    CallBackObjPtr<TradeInfo> callback_obj_ptr = std::move(candle_stick_callback_ptr);
    return callback_obj_ptr;
}


template<EXCHANGE EX>
CallBackObjPtr<TradeInfo> CallBackObjFactory::create_trade_function_callback(const std::string& name) {
    if(name == "debug_trade"){
        ClosePrice config_of_closeprice;
        config_of_closeprice.init();
        std::vector<double> closeprices;
        if constexpr (EX == EXCHANGE::SH) {
            closeprices = config_of_closeprice.sh;
            return create_debug_trade_function(static_cast<int>(closeprices.size()));
        } else if constexpr (EX == EXCHANGE::SZ) {
            closeprices = config_of_closeprice.sz;
            return create_debug_trade_function(static_cast<int>(closeprices.size()));
        } else {
            throw std::runtime_error("sh_or_sz format error");
        }
    }
    else if(name == "candle_stick") {
        ClosePrice config_of_closeprice;
        config_of_closeprice.init();
        std::vector<double> closeprices;
        if constexpr (EX == EXCHANGE::SH) {
            closeprices = config_of_closeprice.sh;
        } else if constexpr (EX == EXCHANGE::SZ) {
            closeprices = config_of_closeprice.sz;
        } else {
            throw std::runtime_error("sh_or_sz format error");
        }
        return create_candle_stick_callback(closeprices);
    }
    throw std::runtime_error("Invalid callback name");
}

#endif
