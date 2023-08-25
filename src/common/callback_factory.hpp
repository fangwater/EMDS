#ifndef CALLBACK_FACTORY_HPP
#define CALLBACK_FACTORY_HPP
#include "config_type.hpp"
#include "../compute_lib/feature_callback.hpp"
#include "../collect_layer/collector.hpp"
#include "../compute_lib/candle_stick.hpp"

class CallBackObjFactory{
private:
    CallBackObjPtr<TradeInfo> create_candle_stick_callback(std::vector<double>& closeprices);
public:
    CallBackObjPtr<TradeInfo> create_trade_function_callback(std::string name, std::string sh_or_sz);
};

CallBackObjPtr<TradeInfo> CallBackObjFactory::create_trade_function_callback(std::string name, std::string sh_or_sz) {
    if(name == "candle_stick"){
        ClosePrice config_of_closeprice;
        config_of_closeprice.init();
        if(sh_or_sz == "sh"){
            std::vector<double> closeprices = config_of_closeprice.sh;
            return create_candle_stick_callback(closeprices);
        }else if(sh_or_sz == "sz"){
            std::vector<double> closeprices = config_of_closeprice.sz;
            return create_candle_stick_callback(closeprices);
        }else{
            throw std::runtime_error("sh_or_sz format error");
        }
    }
}

CallBackObjPtr<TradeInfo> CallBackObjFactory::create_candle_stick_callback(std::vector<double> &closeprices) {
    auto candle_stick_callback_ptr = std::make_unique<CandleStick>(closeprices);
    CallBackObjPtr<TradeInfo> callback_obj_ptr = std::move(candle_stick_callback_ptr);
    return callback_obj_ptr;
}



#endif
