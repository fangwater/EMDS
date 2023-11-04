#ifndef TRADE_AMOUNT
#define TRADE_AMOUNT
#include <tuple>
#include <array>
#include <cstdint>
#include <memory>
#include <vector>
#include <xsimd/xsimd.hpp>
#include "../common/info.hpp"
#include "df_alg.hpp"
#include "feature_callback.hpp"

class TradeAmount : public FeatureCallBack<TradeInfo>{
public:
    enum class ThresholdType{
        DOLLAR,
        STD,
        QUANTILE
    };
private:
    ThresholdType threshold_type;
    std::vector<double> last_tradp;
public:
    explicit TradeAmount(TradeAmount::ThresholdType type, int id_count):threshold_type(type),last_tradp(id_count){};
    virtual std::vector<double> calculate(const PackedInfoSp<TradeInfo>& tick_buffer_sp,int security_id_index) override;
private:    
    std::vector<double> calc_trade_amount(std::shared_ptr<std::vector<std::shared_ptr<TradeInfo>>> packed_ticker_sp,int security_id_index){
        if(packed_ticker_sp->size()==0){
            return std::vector<double>(13,0);
        }
        auto aa_tradp = std::make_unique<std::vector<double>>();
        auto aa_tradv = std::make_unique<std::vector<double>>();
        auto aa_buyno = std::make_unique<std::vector<int64_t>>();
        auto aa_sellno = std::make_unique<std::vector<int64_t>>();
        auto aa_B_or_S = std::make_unique<std::vector<int32_t>>();

        auto aa_S_tradp = std::make_unique<std::vector<double>>();
        auto aa_S_tradv = std::make_unique<std::vector<double>>();
        auto aa_S_buyno = std::make_unique<std::vector<int64_t>>();
        auto aa_S_sellno = std::make_unique<std::vector<int64_t>>();

        auto aa_B_tradv = std::make_unique<std::vector<double>>();
        auto aa_B_tradp = std::make_unique<std::vector<double>>();
        auto aa_B_buyno = std::make_unique<std::vector<int64_t>>();
        auto aa_B_sellno = std::make_unique<std::vector<int64_t>>();
        for(const auto& ticker_sp : *packed_ticker_sp){
        if(ticker_sp->TradPrice > 0){
                aa_tradp->emplace_back(ticker_sp->TradPrice);
                aa_tradv->emplace_back(ticker_sp->TradVolume);
                aa_B_or_S->emplace_back(ticker_sp->B_or_S);
                if( ticker_sp->B_or_S == 1 ){
                    aa_B_tradp->emplace_back(ticker_sp->TradPrice);
                    aa_B_tradv->emplace_back(ticker_sp->TradVolume);
                    aa_B_buyno->emplace_back(ticker_sp->BidApplSeqNum);
                    aa_B_sellno->emplace_back(ticker_sp->OfferApplSeqNum);
                }else{
                    aa_S_tradp->emplace_back(ticker_sp->TradPrice);
                    aa_S_tradv->emplace_back(ticker_sp->TradVolume);
                    aa_S_buyno->emplace_back(ticker_sp->BidApplSeqNum);
                    aa_S_sellno->emplace_back(ticker_sp->OfferApplSeqNum);  
                }   
            }
        }
        if(aa_tradp->empty()){
            return std::vector<double>{0,0,0,0,0,0,0,0,0,0};
        }
        
        //aa['tradamt'] = aa['tradp']*aa['tradv']
        auto aa_tradamt = simd_impl::pairwise_mul(aa_tradv,aa_tradp);
        //aa['last_tradp'] = aa.groupby('securityid')['tradp'].transform('shift')
        auto aa_last_tradp = shift(aa_tradp,1,last_tradp[security_id_index]);
        last_tradp[security_id_index] = aa_tradp->back();
        //aa['logret'] = (np.log(aa['tradp']+1) - np.log(aa['last_tradp']+1)).replace([np.inf,-np.inf],np.nan)
        auto aa_logret = [&aa_tradv,&aa_tradp,&aa_last_tradp](){
            int size_of_tradp = aa_tradp->size();
            auto bias_one = std::make_unique<std::vector<double>>(size_of_tradp,1);
            auto x = simd_impl::pairwise_add(aa_tradp,bias_one);
            auto log_x = simd_impl::pairwise_log(x);
            auto y = simd_impl::pairwise_add(aa_last_tradp,bias_one);
            auto log_y = simd_impl::pairwise_log(y);
            auto z = simd_impl::pairwise_sub(log_x,log_y);
            replace_inf_with_nan(z);
            replace_nan_with_zero(z);
            return z;
        }();
        auto [aa_B_logret,aa_S_logret] = [&aa_logret,&aa_B_or_S](){
            auto is_buy = satisfy<int>(aa_B_or_S,[](const int& B_or_S){
                return B_or_S == 1;
            });
            auto is_sell = satisfy<int>(aa_B_or_S,[](const int& B_or_S){
                return B_or_S == -1;
            });
            uint32_t buy_count = simd_impl::sum(is_buy);
            uint32_t sell_count = aa_logret->size() - buy_count;
            auto aa_B_logret = selected_by<double>(aa_logret, is_buy, buy_count);
            auto aa_S_logret = selected_by<double>(aa_logret, is_sell,sell_count);
            return std::make_tuple(std::move(aa_B_logret),std::move(aa_S_logret));
        }();
        //aa_B = aa.loc[aa.bs == 'B']
        //aa_S = aa.loc[aa.bs == 'S']
        //已经按照B_S分好，log_ret处理过

        //aa_B['tradamt_sum'] = aa_B.groupby('buyno')['tradamt'].transform('sum')
        //aa_S['tradamt_sum'] = aa_S.groupby('sellno')['tradamt'].transform('sum')
        auto aa_B_tradamt = simd_impl::pairwise_mul(aa_B_tradv,aa_B_tradp);
        auto [aa_B_tradamt_sum,aa_B_tradv_sum,aa_B_logret_sum] = [&]() {
            auto res = group::by(aa_B_buyno);
            auto partition_res_amt = group::partition(res, aa_B_tradamt);
            auto partition_res_tradv = group::partition(res, aa_B_tradv);
            auto partition_res_logret = group::partition(res, aa_B_logret);
            auto amt_sum = group::transform::sum(res, partition_res_amt);
            auto tradv_sum = group::transform::sum(res, partition_res_tradv);
            auto logret_sum = group::transform::sum(res, partition_res_logret);
            return std::tuple(std::move(amt_sum),std::move(tradv_sum),std::move(logret_sum));
        }();

        auto aa_S_tradamt = simd_impl::pairwise_mul(aa_S_tradv,aa_S_tradp);
        auto [aa_S_tradamt_sum,aa_S_tradv_sum,aa_S_logret_sum] = [&]() {
            auto res = group::by(aa_S_sellno);
            auto partition_res_amt = group::partition(res, aa_S_tradamt);
            auto partition_res_tradv = group::partition(res, aa_S_tradv);
            auto partition_res_logret = group::partition(res, aa_S_logret);
            auto amt_sum = group::transform::sum(res, partition_res_amt);
            auto tradv_sum = group::transform::sum(res, partition_res_tradv);
            auto logret_sum = group::transform::sum(res, partition_res_logret);
            return std::tuple(std::move(amt_sum),std::move(tradv_sum),std::move(logret_sum));
        }();

        //aa_B = aa_B.groupby('buyno')['tradp_avg','tradv_sum','tradamt_sum','logret_sum'].first() 
        std::tie(aa_B_tradv_sum, aa_B_tradamt_sum, aa_B_logret_sum) = 
            [&aa_B_buyno,&aa_B_tradv_sum, &aa_B_tradamt_sum, &aa_B_logret_sum](){
            auto res = group::by(aa_B_buyno);
            // auto partition_res_tradp_avg = group::partition(res, aa_B_tradp_avg);
            auto partition_res_tradv_sum = group::partition(res, aa_B_tradv_sum);
            auto partition_res_tradamt_sum = group::partition(res, aa_B_tradamt_sum);
            auto partition_res_logret_sum = group::partition(res, aa_B_logret_sum);
            // auto tradp_avg = group::transform::first(res, partition_res_tradp_avg);
            auto tradv_sum = group::aggregate::first(res, partition_res_tradv_sum);
            auto tradamt_sum = group::aggregate::first(res, partition_res_tradamt_sum);
            auto logret_sum = group::aggregate::first(res, partition_res_logret_sum);
            return std::make_tuple(
                // std::move(tradp_avg), 
                std::move(tradv_sum), 
                std::move(tradamt_sum), 
                std::move(logret_sum)
            );
        }();

        //aa_S = aa_S.groupby('sellno')['tradp_avg','tradv_sum','tradamt_sum','logret_sum'].first()
        std::tie(aa_S_tradv_sum, aa_S_tradamt_sum, aa_S_logret_sum) = 
            [&aa_S_sellno,&aa_S_tradv_sum, &aa_S_tradamt_sum, &aa_S_logret_sum](){
            auto res = group::by(aa_S_sellno);
            // auto partition_res_tradp_avg = group::partition(res, aa_S_tradp_avg);
            auto partition_res_tradv_sum = group::partition(res, aa_S_tradv_sum);
            auto partition_res_tradamt_sum = group::partition(res, aa_S_tradamt_sum);
            auto partition_res_logret_sum = group::partition(res, aa_S_logret_sum);
            // auto tradp_avg = group::transform::first(res, partition_res_tradp_avg);
            auto tradv_sum = group::aggregate::first(res, partition_res_tradv_sum);
            auto tradamt_sum = group::aggregate::first(res, partition_res_tradamt_sum);
            auto logret_sum = group::aggregate::first(res, partition_res_logret_sum);
            return std::make_tuple(
                // std::move(tradp_avg), 
                std::move(tradv_sum), 
                std::move(tradamt_sum), 
                std::move(logret_sum)
            );
        }();

        auto aa_order_tradamt_sum =  concat::direct<double>(aa_B_tradamt_sum,aa_S_tradamt_sum);
        double threshold_ultra;
        if(threshold_type == ThresholdType::DOLLAR){
            threshold_ultra = 1000000;
        }else if(threshold_type == ThresholdType::STD){
            threshold_ultra = [&aa_order_tradamt_sum](){
                auto x = simd_impl::mean(aa_order_tradamt_sum);
                auto y = simd_impl::std(aa_order_tradamt_sum,x);
                return x + 2.5*y;
            }();
        }else if(threshold_type == ThresholdType::QUANTILE){
            threshold_ultra = quantile(aa_order_tradamt_sum,0.99);
        }else{
            throw std::runtime_error("Unknown ThresholdType");
        }
        
        auto ultraBuyAmt = [&aa_B_tradamt_sum,&threshold_ultra](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_B_tradamt_sum->size(); i++){
                if((*aa_B_tradamt_sum)[i] > threshold_ultra){
                    amt_sum += (*aa_B_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto ultraSellAmt = [&aa_S_tradamt_sum,&threshold_ultra](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_S_tradamt_sum->size(); i++){
                if((*aa_S_tradamt_sum)[i] > threshold_ultra){
                    amt_sum += (*aa_S_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();
        auto aa_order_logret_sum = concat::direct<double>(aa_B_logret_sum,aa_S_logret_sum);

        auto ultraOrderchg = [&aa_order_logret_sum, &aa_order_tradamt_sum,&threshold_ultra](){
            double logret_sum = 0.0;
            for(int i = 0; i < aa_order_tradamt_sum->size(); i++){
                if((*aa_order_tradamt_sum)[i] > threshold_ultra){
                    //std::cout << fmt::format("{:.3f}",(*aa_order_logret_sum)[i])<< std::endl;
                    logret_sum += (*aa_order_logret_sum)[i];
                }
            }
            return logret_sum;
        }();

        double threshold_big;
        if(threshold_type == ThresholdType::DOLLAR){
            threshold_big = 200000;
        }else if(threshold_type == ThresholdType::STD){
            threshold_big = [&aa_order_tradamt_sum](){
                auto x = simd_impl::mean(aa_order_tradamt_sum);
                auto y = simd_impl::std(aa_order_tradamt_sum,x);
                return x+y;
            }();
        }else if(threshold_type == ThresholdType::QUANTILE){
            threshold_big = quantile(aa_order_tradamt_sum,0.95);
            //std::cout << fmt::format("threshold_big: {:.10f}",threshold_big) << std::endl;
        }else{
            throw std::runtime_error("Unknown ThresholdType");
        }

        auto bigBuyAmt = [&aa_B_tradamt_sum,&threshold_ultra,&threshold_big](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_B_tradamt_sum->size(); i++){
                if((*aa_B_tradamt_sum)[i] <= threshold_ultra && (*aa_B_tradamt_sum)[i] > threshold_big){
                    amt_sum += (*aa_B_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto bigSellAmt = [&aa_S_tradamt_sum,&threshold_ultra,&threshold_big](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_S_tradamt_sum->size(); i++){
                if((*aa_S_tradamt_sum)[i] <= threshold_ultra && (*aa_S_tradamt_sum)[i] > threshold_big){
                    amt_sum += (*aa_S_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto bigOrderchg = [&aa_order_logret_sum, &aa_order_tradamt_sum,&threshold_ultra,&threshold_big](){
            double logret_sum = 0.0;
            for(int i = 0; i < aa_order_tradamt_sum->size(); i++){
                if((*aa_order_tradamt_sum)[i] <= threshold_ultra && (*aa_order_tradamt_sum)[i] > threshold_big){
                    logret_sum += (*aa_order_logret_sum)[i];
                }
            }
            return logret_sum;
        }();

        auto ultrabigOrderchg = [&aa_order_logret_sum,&aa_order_tradamt_sum,&threshold_ultra,&threshold_big](){
            double logret_sum = 0.0;
            for(int i = 0; i < aa_order_tradamt_sum->size(); i++){
                if((*aa_order_tradamt_sum)[i] > threshold_big){
                    logret_sum += (*aa_order_logret_sum)[i];
                }
            }
            return logret_sum;
        }();


        double threshold_mid;
        if(threshold_type == ThresholdType::DOLLAR){
            threshold_mid = 40000;
        }else if(threshold_type == ThresholdType::STD){
            threshold_mid = simd_impl::mean(aa_order_tradamt_sum);
        }else if(threshold_type == ThresholdType::QUANTILE){
            threshold_mid = quantile(aa_order_tradamt_sum,0.5);
        }else{
            throw std::runtime_error("Unknown ThresholdType");
        }

        auto midBuyAmt = [&aa_B_tradamt_sum,&threshold_big,&threshold_mid](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_B_tradamt_sum->size(); i++){
                if((*aa_B_tradamt_sum)[i] <= threshold_big && (*aa_B_tradamt_sum)[i] > threshold_mid){
                    amt_sum += (*aa_B_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto midSellAmt = [&aa_S_tradamt_sum,&threshold_big,&threshold_mid](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_S_tradamt_sum->size(); i++){
                if((*aa_S_tradamt_sum)[i] <= threshold_big && (*aa_S_tradamt_sum)[i] > threshold_mid){
                    amt_sum += (*aa_S_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto midOrderchg = [&aa_order_logret_sum, &aa_order_tradamt_sum,&threshold_big, &threshold_mid](){
            double logret_sum = 0.0;
            for(int i = 0; i < aa_order_tradamt_sum->size(); i++){
                if((*aa_order_tradamt_sum)[i] <= threshold_big && (*aa_order_tradamt_sum)[i] > threshold_mid){
                    logret_sum += (*aa_order_logret_sum)[i];
                }
            }
            return logret_sum;
        }();

        auto smallBuyAmt = [&aa_B_tradamt_sum,&threshold_mid](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_B_tradamt_sum->size(); i++){
                if((*aa_B_tradamt_sum)[i] <= threshold_mid){
                    amt_sum += (*aa_B_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto smallSellAmt = [&aa_S_tradamt_sum, &threshold_mid](){
            double amt_sum = 0.0;
            for(int i = 0; i < aa_S_tradamt_sum->size(); i++){
                if((*aa_S_tradamt_sum)[i] <= threshold_mid){
                    amt_sum += (*aa_S_tradamt_sum)[i];
                }
            }
            return amt_sum;
        }();

        auto smallOrderchg = [&aa_order_logret_sum, &aa_order_tradamt_sum,&threshold_mid](){
            double logret_sum = 0.0;
            for(int i = 0; i < aa_order_tradamt_sum->size(); i++){
                if((*aa_order_tradamt_sum)[i] <= threshold_mid){
                    logret_sum += (*aa_order_logret_sum)[i];
                }
            }
            return logret_sum;
        }();
        return std::vector<double>{ultraBuyAmt,ultraSellAmt,ultraOrderchg,bigBuyAmt,bigSellAmt,bigOrderchg,ultrabigOrderchg,midBuyAmt,midSellAmt,midOrderchg,smallBuyAmt,smallSellAmt,smallOrderchg};
    }
};

std::vector<double> TradeAmount::calculate(const PackedInfoSp<TradeInfo> &tick_buffer_sp, int security_id_index) {
    return calc_trade_amount(tick_buffer_sp,security_id_index);
}
#endif
