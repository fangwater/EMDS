#ifndef CANDLE_STICK
#define CANDLE_STICK
#include <tuple>
#include <array>
#include <cstdint>
#include <memory>
#include <vector>
#include <xsimd/xsimd.hpp>
#include "../common/info.hpp"
#include "df_alg.hpp"
#include "feature_callback.hpp"

class CandleStick : public FeatureCallBack<TradeInfo>{
private:
    std::vector<double> previous_close_price;
public:
    explicit CandleStick(std::vector<double>& closeprices){
        previous_close_price = closeprices;
    }
    virtual std::vector<double> calculate(const PackedInfoSp<TradeInfo>& tick_buffer_sp,int security_id_index) override;
};

std::vector<double> CandleStick::calculate(const PackedInfoSp<TradeInfo>& tick_buffer_sp,int security_id_index){
    auto ele_count = tick_buffer_sp->size();
    if(ele_count == 0){
        double inheritance_price = previous_close_price[security_id_index];
        return std::vector<double>{0,0,0,0,0,0,0,0,0,0,0,inheritance_price,inheritance_price};
    }
    auto aa_tradp = std::make_unique<std::vector<double,xsimd::aligned_allocator<double>>>(ele_count);
    auto aa_S_tradp = std::make_unique<std::vector<double,xsimd::aligned_allocator<double>>>();
    aa_S_tradp->reserve(ele_count);
    auto aa_B_tradp = std::make_unique<std::vector<double,xsimd::aligned_allocator<double>>>();
    aa_B_tradp->reserve(ele_count);
    auto aa_tradv = std::make_unique<std::vector<double,xsimd::aligned_allocator<double>>>(ele_count);
    auto aa_S_tradv = std::make_unique<std::vector<double,xsimd::aligned_allocator<double>>>();
    aa_S_tradv->reserve(ele_count);
    auto aa_B_tradv = std::make_unique<std::vector<double,xsimd::aligned_allocator<double>>>();
    aa_B_tradv->reserve(ele_count);
    for(int i = 0; i < tick_buffer_sp->size(); i++){
        std::shared_ptr<TradeInfo> ticker_sp = tick_buffer_sp->at(i);
        if(ticker_sp->TradPrice > 0){
            aa_tradp->at(i) = ticker_sp->TradPrice;
            aa_tradv->at(i) = ticker_sp->TradVolume;
            if( ticker_sp->B_or_S == 1 ){
                aa_B_tradp->emplace_back(ticker_sp->TradPrice);
                aa_B_tradv->emplace_back(ticker_sp->TradVolume);
            }else{
                aa_S_tradp->emplace_back(ticker_sp->TradPrice);
                aa_S_tradv->emplace_back(ticker_sp->TradVolume);
            }
        }
    }
    if(aa_tradp->size() == 0){
        double inheritance_price = previous_close_price[security_id_index];
        return std::vector<double>{0,0,0,0,0,0,0,0,0,0,0,inheritance_price,inheritance_price};
    }
    auto cjb = static_cast<double>(aa_tradp->size());
    auto bcjb = static_cast<double>(aa_B_tradp->size());
    auto scjb = static_cast<double>(aa_S_tradp->size());
    auto aa_tradmt = xsimd_aligned::pairwise_mul(aa_tradp,aa_tradv);
    auto aa_B_tradmt = xsimd_aligned::pairwise_mul(aa_B_tradp,aa_B_tradv);
    auto aa_S_tradmt = xsimd_aligned::pairwise_mul(aa_S_tradp,aa_S_tradv);
    auto volume_nfq = xsimd_aligned::sum(aa_tradv);
    auto bvolume_nfq = xsimd_aligned::sum(aa_B_tradv);
    auto svolume_nfq = xsimd_aligned::sum(aa_S_tradv);
    auto amount = xsimd_aligned::sum(aa_tradmt);
    auto bamount = xsimd_aligned::sum(aa_B_tradmt);
    auto samount = xsimd_aligned::sum(aa_S_tradmt);
    auto [lowprice_nfq, highprice_nfq, openprice_nfq, closeprice_nfq] = xsimd_aligned::min_max_start_end(aa_tradp);
    previous_close_price[security_id_index] = closeprice_nfq;
    return std::vector<double>{cjb,bcjb,scjb,volume_nfq,bvolume_nfq,svolume_nfq,amount,bamount,samount,lowprice_nfq,highprice_nfq,openprice_nfq,closeprice_nfq};
}

#endif
