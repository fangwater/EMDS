#ifndef CALLBACK_FACTORY_HPP
#define CALLBACK_FACTORY_HPP
#include "config_type.hpp"
#include "../collect_layer/collector.hpp"
#include "../compute_lib/feature_callback.hpp"
#include "../compute_lib/trade_debug.hpp"
#include "../compute_lib/candle_stick.hpp"
#include "../compute_lib/trade_amount.hpp"
#include "../compute_lib/order_flow_min.hpp"

class CallBackObjFactory{
private:
    static CallBackObjPtr<TradeInfo> create_debug_trade_function(int index_count);
    static CallBackObjPtr<TradeInfo> create_candle_stick_callback(std::vector<double>& closeprices);
    //DOLLAR,STD,QUANTILE
    static CallBackObjPtr<TradeInfo> create_trade_amount_callback(TradeAmount::ThresholdType type,int index_count);
    //NONE,BIG,SMALL
    static CallBackObjPtr<TradeInfo> create_order_flow_min_callback(OrderFlowMin::OrderType type,int index_count);
public:
    template<EXCHANGE EX>
    static CallBackObjPtr<TradeInfo> create_trade_function_callback(const std::string& name);
};

CallBackObjPtr<TradeInfo> CallBackObjFactory::create_trade_amount_callback(TradeAmount::ThresholdType type,int index_count){
    auto trade_amount_callback_ptr = std::make_unique<TradeAmount>(type,index_count);
    CallBackObjPtr<TradeInfo> callback_obj_ptr = std::move(trade_amount_callback_ptr);
    return callback_obj_ptr;
}

CallBackObjPtr<TradeInfo> CallBackObjFactory::create_order_flow_min_callback(OrderFlowMin::OrderType type, int index_count) {
    auto order_flow_min_callback_ptr = std::make_unique<OrderFlowMin>(type,index_count);
    CallBackObjPtr<TradeInfo> callback_obj_ptr = std::move(order_flow_min_callback_ptr);
    return callback_obj_ptr;
}

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
    // FeatureOption feature_option;
    // feature_option.init();
    int id_count = [&](){
        ClosePrice config_of_closeprice;
        config_of_closeprice.init();
        if constexpr (EX == EXCHANGE::SH) {
            return config_of_closeprice.sh.size();
        }else if constexpr (EX == EXCHANGE::SZ) {
            return config_of_closeprice.sz.size();
        }else{
            throw std::runtime_error("sh_or_sz format error");
        }
    }();
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
    else if(name == "trade_amount_dollar"){
        return create_trade_amount_callback(TradeAmount::ThresholdType::DOLLAR,id_count);
    }else if(name == "trade_amount_std"){
        return create_trade_amount_callback(TradeAmount::ThresholdType::STD,id_count);
    }else if(name == "trade_amount_quantile"){
        return create_trade_amount_callback(TradeAmount::ThresholdType::QUANTILE,id_count);
    }else if(name == "order_flow_min"){
        return create_order_flow_min_callback(OrderFlowMin::OrderType::NONE,id_count);
    }else if(name == "order_flow_min_big"){
        return create_order_flow_min_callback(OrderFlowMin::OrderType::BIG,id_count);
    }else if(name == "order_flow_min_small"){
        return create_order_flow_min_callback(OrderFlowMin::OrderType::SMALL,id_count);
    }
    else{
        throw std::runtime_error("Invalid callback name");
    }
}

#endif
