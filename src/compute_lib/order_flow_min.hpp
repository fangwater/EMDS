#ifndef ORDER_FLOW_MIN
#define ORDER_FLOW_MIN
#include <iostream>
#include <vector>
#include <cmath>
#include <random>
#include <chrono>
#include <xsimd/xsimd.hpp>
#include <fmt/format.h>
#include <absl/time/time.h>
#include <absl/time/civil_time.h>
#include <absl/strings/str_split.h>
#include <nlohmann/json.hpp>
#include <absl/strings/numbers.h>
#include "../common/info.hpp"
#include "df_alg.hpp"
#include "feature_callback.hpp"

class OrderFlowMinResult{
public:
    double Sv;
    double Scjbs;
    double Bv;
    double Bcjbs;
    double tradv;
    double cjbs;
    double Samt;
    double Bamt;
    double tradamt;
    double CumTradv;
    double CumCjbs;
    double CumTradamt;
    double Delta;
    double cjbsDelta;
    double MaxDeltaStrength;
    double cjbsMaxDeltaStrength;
    double MinDeltaStrength;
    double cjbsMinDeltaStrength;
    double DeltaStrength_MinMaxRatio;
    double cjbsDeltaStrength_MinMaxRatio;
    double CumDelta;
    double CumCjbsDelta;
    double DeltaStrength;
    double cjbsDeltaStrength;
    double CumDeltaStrength;
    double CumCjbsDeltaStrength;
    double DeltaStrength_avg;
    double cjbsDeltaStrength_avg;
    double SD_ratio_avg;
    double SDcjbs_ratio_avg;
    double SD_ratio_max;
    double SDcjbs_ratio_max;
    double SD_ratio_min;
    double SDcjbs_ratio_min;
    double SD_ratio;
    double SDcjbs_ratio;
    double S_equilibrium_P;
    double Scjbs_equilibrium_P;
    double D_equilibrium_P;
    double Dcjbs_equilibrium_P;
    double S_equilibrium;
    double Scjbs_equilibrium;
    double D_equilibrium;
    double Dcjbs_equilibrium;
    double S_equilibrium_ratio;
    double Scjbs_equilibrium_ratio;
    double D_equilibrium_ratio;
    double Dcjbs_equilibrium_ratio;
    double S_equilibrium_count;
    double Scjbs_equilibrium_count;
    double D_equilibrium_count;
    double Dcjbs_equilibrium_count;
    double POC;
    double cjbsPOC;
    double SPOC;
    double ScjbsPOC;
    double BPOC;
    double BcjbsPOC;
    double Svwap;
    double Bvwap;
    double S_orderDecay;
    double Scjbs_orderDecay;
    double D_orderDecay;
    double Dcjbs_orderDecay;
    double S_decaySlope;
    double Scjbs_decaySlope;
    double D_decaySlope;
    double Dcjbs_decaySlope;
public:
    std::vector<double> to_vector() const {
        return {
            Sv, Scjbs, Bv, Bcjbs, tradv, cjbs, Samt, Bamt, tradamt, CumTradv,
            CumCjbs, CumTradamt, Delta, cjbsDelta, MaxDeltaStrength, cjbsMaxDeltaStrength, MinDeltaStrength, 
            cjbsMinDeltaStrength, DeltaStrength_MinMaxRatio, cjbsDeltaStrength_MinMaxRatio, CumDelta, CumCjbsDelta,
            DeltaStrength, cjbsDeltaStrength, CumDeltaStrength, CumCjbsDeltaStrength, DeltaStrength_avg, 
            cjbsDeltaStrength_avg, SD_ratio_avg, SDcjbs_ratio_avg, SD_ratio_max, SDcjbs_ratio_max, SD_ratio_min, 
            SDcjbs_ratio_min, SD_ratio, SDcjbs_ratio, S_equilibrium_P, Scjbs_equilibrium_P, D_equilibrium_P, 
            Dcjbs_equilibrium_P, S_equilibrium, Scjbs_equilibrium, D_equilibrium, Dcjbs_equilibrium, 
            S_equilibrium_ratio, Scjbs_equilibrium_ratio, D_equilibrium_ratio, Dcjbs_equilibrium_ratio, 
            S_equilibrium_count, Scjbs_equilibrium_count, D_equilibrium_count, Dcjbs_equilibrium_count, POC, 
            cjbsPOC, SPOC, ScjbsPOC, BPOC, BcjbsPOC, Svwap, Bvwap, S_orderDecay, Scjbs_orderDecay, D_orderDecay, 
            Dcjbs_orderDecay, S_decaySlope, Scjbs_decaySlope, D_decaySlope, Dcjbs_decaySlope
        };
    }
    std::vector<double> empty_vector() const {
        return std::vector<double>(to_vector().size(),std::numeric_limits<double>::quiet_NaN());
    }
    void print() const {
        std::vector<double> res = to_vector();
        std::vector<const char*> names = {
            "Sv", "Scjbs", "Bv", "Bcjbs", "tradv", "cjbs", "Samt", "Bamt", "tradamt", "CumTradv",
            "CumCjbs", "CumTradamt", "Delta", "cjbsDelta", "MaxDeltaStrength", "cjbsMaxDeltaStrength", 
            "MinDeltaStrength", "cjbsMinDeltaStrength", "DeltaStrength_MinMaxRatio", "cjbsDeltaStrength_MinMaxRatio", 
            "CumDelta", "CumCjbsDelta", "DeltaStrength", "cjbsDeltaStrength", "CumDeltaStrength", 
            "CumCjbsDeltaStrength", "DeltaStrength_avg", "cjbsDeltaStrength_avg", "SD_ratio_avg", 
            "SDcjbs_ratio_avg", "SD_ratio_max", "SDcjbs_ratio_max", "SD_ratio_min", "SDcjbs_ratio_min", "SD_ratio", 
            "SDcjbs_ratio", "S_equilibrium_P", "Scjbs_equilibrium_P", "D_equilibrium_P", "Dcjbs_equilibrium_P", 
            "S_equilibrium", "Scjbs_equilibrium", "D_equilibrium", "Dcjbs_equilibrium", "S_equilibrium_ratio", 
            "Scjbs_equilibrium_ratio", "D_equilibrium_ratio", "Dcjbs_equilibrium_ratio", "S_equilibrium_count", 
            "Scjbs_equilibrium_count", "D_equilibrium_count", "Dcjbs_equilibrium_count", "POC", "cjbsPOC", 
            "SPOC", "ScjbsPOC", "BPOC", "BcjbsPOC", "Svwap", "Bvwap", "S_orderDecay", "Scjbs_orderDecay", 
            "D_orderDecay", "Dcjbs_orderDecay", "S_decaySlope", "Scjbs_decaySlope", "D_decaySlope", "Dcjbs_decaySlope"
        };
        fmt::print("+------------------+------------------+\n");
        for (size_t i = 0; i < res.size(); ++i) {
            fmt::print("| {:<32} | {:>16.5f} |\n", names[i], res[i]);
        }
        fmt::print("+------------------+------------------+\n");
    }
};

class OrderFlowMin : public FeatureCallBack<TradeInfo>{
public:
    enum class OrderType{
        NONE,
        BIG,
        SMALL
    };
private:
    struct Accumlate {
        double Tradv = 0.0;
        double Cjbs = 0.0;
        double Tradamt = 0.0;
        double Delta = 0.0;
        double CjbsDelta = 0.0;
    };
    std::vector<Accumlate> accumlate_values;
    OrderType order_type;
public:
    explicit OrderFlowMin(OrderType type, int index_count):order_type(type),accumlate_values(index_count){}
public:
    virtual std::vector<double> calculate(const PackedInfoSp<TradeInfo>& tick_buffer_sp,int security_id_index) override;
    std::vector<double> calc_order_flow_min(std::shared_ptr<std::vector<std::shared_ptr<TradeInfo>>> packed_ticker_sp, int security_id_index);
    static std::vector<double> get_res(const OrderFlowMinResult& ans){
        ans.print();
        return ans.to_vector();
    }
};  

std::vector<double> OrderFlowMin::calculate(const PackedInfoSp<TradeInfo> &tick_buffer_sp, int security_id_index) {
    return calc_order_flow_min(tick_buffer_sp,security_id_index);
}

std::vector<double> OrderFlowMin::calc_order_flow_min(std::shared_ptr<std::vector<std::shared_ptr<TradeInfo>>> packed_ticker_sp,int security_id_index){
    // aa = aa.loc[aa.tradp>0]
    // aa_B = aa.loc[aa.bs == 'B']
    // aa_S = aa.loc[aa.bs == 'S']
    OrderFlowMinResult ans;
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
    if(packed_ticker_sp->size() == 0){
        return ans.empty_vector();
    }
    for(int i = 0; i < packed_ticker_sp->size(); i++){
        std::shared_ptr<TradeInfo> ticker_sp = packed_ticker_sp->at(i);
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
        return ans.empty_vector();
    }
    // fmt::print("aa_tradp size :{}, aa_B_tradp: {}, aa_S_tradp {}\n",aa_tradp->size(),aa_B_tradp->size(),aa_S_tradp->size());
    // DataFrame aa;
    // DataFrame aa_B, aa_S;
    // aa['tradamt'] = aa['tradv'] * aa['tradp']
    auto aa_tradamt = simd_impl::pairwise_mul(aa_tradv,aa_tradp);
    // fmt::print("Before aa_S_tradv->size(): {}\n",aa_S_tradv->size());
    // fmt::print("Before aa_B_tradv->size(): {}\n",aa_B_tradv->size());
    if(order_type != OrderType::NONE){
        // aa_B['tradamt_sum'] = aa_B.groupby('buyno')['tradamt'].transform('sum')
        // aa_S['tradamt_sum'] = aa_S.groupby('sellno')['tradamt'].transform('sum')
        auto aa_B_tradamt = simd_impl::pairwise_mul(aa_B_tradv,aa_B_tradp);
        std::unique_ptr<std::vector<double>> aa_B_tradamt_sum = [&]() {
            auto res = group::by(aa_B_buyno);
            auto partition_res = group::partition(res, aa_B_tradamt);
            return group::transform::sum(res, partition_res);
        }();

        auto aa_S_tradamt = simd_impl::pairwise_mul(aa_S_tradv,aa_S_tradp);
        auto aa_S_tradamt_sum = [&]() {
            auto res = group::by(aa_S_sellno);
            auto partition_res = group::partition(res, aa_S_tradamt);
            return group::transform::sum(res, partition_res);
        }();
        
        auto aa_tradamt_sum = concat::direct(aa_B_tradamt,aa_S_tradamt); 
        double threshlod_percentile = 0.0;
        if(order_type == OrderType::BIG){
            threshlod_percentile = 0.95;
        }else if (order_type == OrderType::SMALL){
            threshlod_percentile = 0.5;
        }else{
            throw std::runtime_error("Unknown OrderType");
        }
        auto threshlod = quantile_without_nan_and_duplicates(aa_tradamt_sum,threshlod_percentile);
        // aa_B = aa_B.loc[aa_B.tradamt_sum>threshold]
        // aa_S = aa_S.loc[aa_S.tradamt_sum>threshold]
        // but only need tradv && tradp
        std::tie(aa_B_tradp, aa_B_tradv) =  [&](){
            auto result = satisfy<double>(aa_B_tradamt_sum, [threshlod](const double& value) {
                return value > threshlod; 
            });
            uint32_t selected_count = simd_impl::sum(result);
            auto tradv_ptr = selected_by<double>(aa_B_tradp, result, selected_count);
            auto tradp_ptr = selected_by<double>(aa_B_tradv, result, selected_count);
            return std::make_tuple(std::move(tradv_ptr), std::move(tradp_ptr));
        }();

        std::tie(aa_S_tradp, aa_S_tradv) =  [&](){
            auto result = satisfy<double>(aa_S_tradamt_sum, [threshlod](const double& value) {
                return value > threshlod; 
            });
            uint32_t selected_count = simd_impl::sum(result);
            auto tradv_ptr = selected_by<double>(aa_S_tradp, result, selected_count);
            auto tradp_ptr = selected_by<double>(aa_S_tradv, result, selected_count);
            return std::make_tuple(std::move(tradv_ptr), std::move(tradp_ptr));
        }();
    }
    if(aa_B_tradv->size() == 0 || aa_S_tradv->size() == 0){
        return ans.empty_vector();
    }
    // orderFlow_S = aa_S.groupby('tradp')['tradv'].sum().to_frame()
    // orderFlow_S.columns = ['Sv']
    // orderFlow_S['Scjbs'] = aa_S.groupby('tradp')['tradv'].count()
    auto [flow_S_price, Sv, Scjbs] = [&]() {
        auto res = group::by(aa_S_tradp);
        auto partition_res = group::partition(res, aa_S_tradv);
        auto sum_ptr = group::aggregate::sum(res, partition_res);
        auto count_ptr = group::aggregate::count_as_double(res);
        auto price = res.get_group_values();
        return std::make_tuple(std::move(price), std::move(sum_ptr), std::move(count_ptr));
    }();
    // orderFlow_B = aa_B.groupby('tradp')['tradv'].sum().to_frame()
    // orderFlow_B.columns = ['Bv']
    // orderFlow_B['Bcjbs'] = aa_B.groupby('tradp')['tradv'].count()
    auto [flow_B_price, Bv, Bcjbs] = [&]() {
        auto res = group::by(aa_B_tradp);
        auto partition_res = group::partition(res, aa_B_tradv);
        auto sum_ptr = group::aggregate::sum(res, partition_res);
        auto count_ptr = group::aggregate::count_as_double(res);
        auto price = res.get_group_values();
        return std::make_tuple(std::move(price), std::move(sum_ptr), std::move(count_ptr));
    }();
    auto [tradp, mapping_1_ptr, mapping_2_ptr] = concat::horizontal(flow_S_price,flow_B_price);
    Sv = concat::apply<double>(mapping_1_ptr, tradp->size(), Sv ,"zero");
    Bv = concat::apply<double>(mapping_2_ptr, tradp->size(), Bv ,"zero");
    Scjbs = concat::apply<double>(mapping_1_ptr, tradp->size(), Scjbs ,"zero");
    Bcjbs = concat::apply<double>(mapping_2_ptr, tradp->size(), Bcjbs ,"zero");
    auto tradv = simd_impl::pairwise_add(Sv, Bv);
    auto cjbs = simd_impl::pairwise_add(Scjbs, Bcjbs);
    auto Samt = simd_impl::pairwise_mul(Sv, tradp);
    auto Bamt = simd_impl::pairwise_mul(Bv, tradp);
    auto tradamt = simd_impl::pairwise_mul(tradv, tradp);
    ans.Sv = simd_impl::sum(Sv);    
    ans.Scjbs = simd_impl::sum(Scjbs);
    ans.Bv = simd_impl::sum(Bv);
    ans.Bcjbs = simd_impl::sum(Bcjbs);
    ans.tradv = ans.Bv + ans.Sv;
    ans.cjbs = ans.Bcjbs + ans.Scjbs;
    ans.Samt = simd_impl::sum(Samt);
    ans.Bamt = simd_impl::sum(Bamt);
    ans.tradamt = simd_impl::sum(tradamt);
    auto& accumlate_value = accumlate_values[security_id_index];
    accumlate_value.Tradv += ans.tradv;
    ans.CumTradv = accumlate_value.Tradv;
    accumlate_value.Cjbs += ans.cjbs;
    ans.CumCjbs = accumlate_value.Cjbs;
    accumlate_value.Tradamt += ans.tradamt;
    ans.CumTradamt = accumlate_value.Tradamt;
    /*bv && sv size = 0*/
    auto Delta = simd_impl::pairwise_sub(Bv, Sv);
    auto cjbsDelta = simd_impl::pairwise_sub(Bcjbs, Scjbs);
    auto DeltaStrength  = simd_impl::pairwise_div(Delta, tradv);
    replace_inf_with_nan(DeltaStrength);
    auto cjbsDeltaStrength = simd_impl::pairwise_div(cjbsDelta,cjbs);
    replace_inf_with_nan(cjbsDeltaStrength);
    ans.Delta = simd_impl::sum(Delta);
    ans.cjbsDelta = simd_impl::sum(cjbsDelta);
    ans.MaxDeltaStrength = simd_impl::max(DeltaStrength);
    ans.cjbsMaxDeltaStrength = simd_impl::max(cjbsDeltaStrength);
    ans.MinDeltaStrength = simd_impl::min(DeltaStrength);
    ans.cjbsMinDeltaStrength = simd_impl::min(cjbsDeltaStrength);
    
    std::tie(ans.DeltaStrength_MinMaxRatio, ans.cjbsDeltaStrength_MinMaxRatio) = [&](){
        if(simd_impl::min(cjbsDelta) != 0){
            return std::make_tuple(simd_impl::max(Delta)/simd_impl::min(Delta),simd_impl::max(cjbsDelta)/simd_impl::min(cjbsDelta));
        }else{
            return std::make_tuple(std::numeric_limits<double>::quiet_NaN(),std::numeric_limits<double>::quiet_NaN());
        }
    }();
    // accumlate_value['CumDelta'] = accumlate_value['CumDelta'] + res['Delta']
    // res['CumDelta'] = res['Delta'].cumsum()
    // accumlate_value['CumCjbsDelta'] = accumlate_value['CumCjbsDelta'] + res['cjbsDelta']
    // res['CumCjbsDelta'] = res['cjbsDelta'].cumsum()
    accumlate_value.Delta += ans.Delta;
    ans.CumDelta = accumlate_value.Delta;

    accumlate_value.CjbsDelta += ans.cjbsDelta;
    ans.CumCjbsDelta = accumlate_value.CjbsDelta;

    // res['DeltaStrength'] = res['Delta']/res['tradv'].replace([np.inf,-np.inf],np.nan)
    // res['cjbsDeltaStrength'] = res['cjbsDelta']/res['cjbs'].replace([np.inf,-np.inf],np.nan)
    // res['CumDeltaStrength'] = res['CumDelta']/res['CumTradv'].replace([np.inf,-np.inf],np.nan)
    // res['CumCjbsDeltaStrength'] = res['CumCjbsDelta']/res['CumCjbs'].replace([np.inf,-np.inf],np.nan)
    ans.DeltaStrength = division_no_inf(ans.Delta , ans.tradv);
    ans.cjbsDeltaStrength = division_no_inf(ans.cjbsDelta , ans.cjbs);
    ans.CumDeltaStrength = division_no_inf(ans.CumDelta , ans.CumTradv);
    ans.CumCjbsDeltaStrength = division_no_inf(ans.CumCjbsDelta , ans.CumCjbs);

    
    // res['DeltaStrength_avg'] = orderFlow['DeltaStrength'].mean()
    // res['cjbsDeltaStrength_avg'] = orderFlow['cjbsDeltaStrength'].mean()
    ans.DeltaStrength_avg = simd_impl::mean(DeltaStrength);
    ans.cjbsDeltaStrength_avg = simd_impl::mean(cjbsDeltaStrength);
    // orderFlow['SD_ratio'] = (orderFlow['Bv']/shift(orderFlow['Sv'], 1, cval=np.NaN)).replace([np.inf,-np.inf],np.nan)
    auto SD_ratio = simd_impl::pairwise_div(Bv, shift(Sv , 1 , std::numeric_limits<double>::quiet_NaN()));
    replace_inf_with_nan(SD_ratio);

    auto SDcjbs_ratio = simd_impl::pairwise_div(Bcjbs, shift(Scjbs, 1, std::numeric_limits<double>::quiet_NaN()));
    replace_inf_with_nan(SDcjbs_ratio);

    //orderFlow['S_equilibrium'] = (orderFlow['Sv']> 3*shift(orderFlow['Bv'], -1, cval=np.NaN)).replace([True,False],[1,0])
    auto S_equilibrium = [&](){
        auto tmp_ptr = shift(Bv, -1, std::numeric_limits<double>::quiet_NaN());
        auto result = satisfy<double>(Sv, tmp_ptr, [](const double& v1, const double& v2) {
            return v1 > 3*v2; 
        });
        return result;
    }();

    // orderFlow['Scjbs_equilibrium'] = (orderFlow['Scjbs']> 3*shift(orderFlow['Bcjbs'], -1, cval=np.NaN)).replace([True,False],[1,0])
    auto Scjbs_equilibrium = [&](){
        auto tmp_ptr = shift(Bcjbs, -1, std::numeric_limits<double>::quiet_NaN());
        auto result = satisfy<double>(Scjbs, tmp_ptr, [](const double& v1, const double& v2) {
            return v1 > 3*v2; 
        });
        return result;
    }();

    // orderFlow['D_equilibrium'] = (orderFlow['Bv']> 3*shift(orderFlow['Sv'], 1, cval=np.NaN)).replace([True,False],[1,0])
    auto D_equilibrium = [&](){
        auto tmp_ptr = shift(Sv, 1, std::numeric_limits<double>::quiet_NaN());
        auto result = satisfy<double>(Bv, tmp_ptr, [](const double& v1, const double& v2){
            return v1 > 3*v2; 
        });
        return result;
    }();
    


    // orderFlow['Dcjbs_equilibrium'] = (orderFlow['Bcjbs']> 3*shift(orderFlow['Scjbs'], 1, cval=np.NaN)).replace([True,False],[1,0])
    auto Dcjbs_equilibrium = [&](){
        auto tmp_ptr = shift(Scjbs, 1, std::numeric_limits<double>::quiet_NaN());
        auto result = satisfy<double>(Bcjbs, tmp_ptr, [](const double& v1, const double& v2){
            return v1 > 3*v2; 
        });
        return result;
    }();
    // def count_helper(slice):
    //     return (slice.groupby(slice.ne(slice.shift()).cumsum()).cumcount()+1)*slice
    // orderFlow['S_equilibrium_count'] = count_helper(orderFlow['S_equilibrium'])
    // orderFlow['Scjbs_equilibrium_count'] = count_helper(orderFlow['Scjbs_equilibrium'])
    // orderFlow['D_equilibrium_count'] = count_helper(orderFlow['D_equilibrium'])
    // orderFlow['Dcjbs_equilibrium_count'] = count_helper(orderFlow['Dcjbs_equilibrium'])
    auto S_equilibrium_count = consecutive_sum(S_equilibrium);
    auto Scjbs_equilibrium_count = consecutive_sum(Scjbs_equilibrium);
    auto D_equilibrium_count = consecutive_sum(D_equilibrium);
    auto Dcjbs_equilibrium_count = consecutive_sum(Dcjbs_equilibrium);

    // res['SD_ratio_avg'] = orderFlow['SD_ratio'].mean()
    // res['SDcjbs_ratio_avg'] = orderFlow['SDcjbs_ratio'].mean()
    ans.SD_ratio_avg = nan_ignore_impl::mean(SD_ratio);
    ans.SDcjbs_ratio_avg = nan_ignore_impl::mean(SDcjbs_ratio);

    // res['SD_ratio_max'] = orderFlow['SD_ratio'].max()
    // res['SDcjbs_ratio_max'] = orderFlow['SDcjbs_ratio'].max()
    ans.SD_ratio_max = nan_ignore_impl::max(SD_ratio);
    ans.SDcjbs_ratio_max = nan_ignore_impl::max(SDcjbs_ratio);


    // res['SD_ratio_min'] = orderFlow['SD_ratio'].min()
    // res['SDcjbs_ratio_min'] = orderFlow['SDcjbs_ratio'].min()
    // ans.SD_ratio_min = simd_impl::min(SD_ratio);
    // ans.SDcjbs_ratio_min = simd_impl::min(SDcjbs_ratio);
    ans.SD_ratio_min = nan_ignore_impl::min(SD_ratio);
    ans.SDcjbs_ratio_min = nan_ignore_impl::min(SDcjbs_ratio);

    // res['SD_ratio'] = ((res['SD_ratio_max']-res['SD_ratio_min'])/(res['SD_ratio_max']+res['SD_ratio_min']))
    // res['SD_ratio'] = replace_inf(res['SD_ratio'])
    // res['SDcjbs_ratio'] = ((res['SDcjbs_ratio_max']-res['SDcjbs_ratio_min'])/(res['SDcjbs_ratio_max']+res['SDcjbs_ratio_min']))
    // res['SDcjbs_ratio'] = replace_inf(res['SDcjbs_ratio'])
    ans.SD_ratio = division_no_inf(ans.SD_ratio_max - ans.SD_ratio_min, ans.SD_ratio_max + ans.SD_ratio_min);
    ans.SDcjbs_ratio = division_no_inf(ans.SDcjbs_ratio_max - ans.SDcjbs_ratio_min, ans.SDcjbs_ratio_max + ans.SDcjbs_ratio_min);

    // res['S_equilibrium_P'] = replace_inf(orderFlow.loc[orderFlow.S_equilibrium==1]['Samt'].sum()/orderFlow.loc[orderFlow.S_equilibrium==1]['Sv'].sum())
    ans.S_equilibrium_P = [&](){
        auto result = satisfy<uint32_t>(S_equilibrium,[](const uint32_t& v){
            return v == 1; 
        });
        uint32_t selected_count = simd_impl::sum(result);
        auto ptr1 = selected_by<double>(Samt, result, selected_count);
        auto ptr2 = selected_by<double>(Sv, result, selected_count);
        return division_no_inf(simd_impl::sum(ptr1),simd_impl::sum(ptr2));
    }();
    // res['Scjbs_equilibrium_P'] = replace_inf(orderFlow.loc[orderFlow.Scjbs_equilibrium==1]['Samt'].sum()/orderFlow.loc[orderFlow.Scjbs_equilibrium==1]['Sv'].sum())
    ans.Scjbs_equilibrium_P = [&](){
        auto result = satisfy<uint32_t>(Scjbs_equilibrium,[](const uint32_t& v){
            return v == 1; 
        });
        uint32_t selected_count = simd_impl::sum(result);
        auto ptr1 = selected_by<double>(Samt, result, selected_count);
        auto ptr2 = selected_by<double>(Sv, result, selected_count);
        return division_no_inf(simd_impl::sum(ptr1),simd_impl::sum(ptr2));
    }();
    // res['D_equilibrium_P'] = replace_inf(orderFlow.loc[orderFlow.D_equilibrium==1]['Bamt'].sum()/orderFlow.loc[orderFlow.D_equilibrium==1]['Bv'].sum())
    ans.D_equilibrium_P = [&](){
        auto result = satisfy<uint32_t>(D_equilibrium,[](const uint32_t& v){
            return v == 1; 
        });
        uint32_t selected_count = simd_impl::sum(result);
        auto ptr1 = selected_by<double>(Bamt, result, selected_count);
        auto ptr2 = selected_by<double>(Bv, result, selected_count);
        return division_no_inf(simd_impl::sum(ptr1),simd_impl::sum(ptr2));
    }();
    // res['Dcjbs_equilibrium_P'] = replace_inf(orderFlow.loc[orderFlow.Dcjbs_equilibrium==1]['Bamt'].sum()/orderFlow.loc[orderFlow.Dcjbs_equilibrium==1]['Bv'].sum())
    ans.Dcjbs_equilibrium_P = [&](){
        auto result = satisfy<uint32_t>(Dcjbs_equilibrium,[](const uint32_t& v){
            return v == 1; 
        });
        uint32_t selected_count = simd_impl::sum(result);
        auto ptr1 = selected_by<double>(Bamt, result, selected_count);
        auto ptr2 = selected_by<double>(Bv, result, selected_count);
        return division_no_inf(simd_impl::sum(ptr1),simd_impl::sum(ptr2));
    }();

    // res['S_equilibrium'] = orderFlow['S_equilibrium'].sum()
    ans.S_equilibrium = simd_impl::sum(S_equilibrium);

    // res['Scjbs_equilibrium'] = orderFlow['Scjbs_equilibrium'].sum()
    ans.Scjbs_equilibrium = simd_impl::sum(Scjbs_equilibrium);

    // res['D_equilibrium'] = orderFlow['D_equilibrium'].sum()
    ans.D_equilibrium = simd_impl::sum(D_equilibrium);

    // res['Dcjbs_equilibrium'] = orderFlow['Dcjbs_equilibrium'].sum()
    ans.Dcjbs_equilibrium = simd_impl::sum(Dcjbs_equilibrium);

    // res['S_equilibrium_ratio'] = replace_inf(res['S_equilibrium']/orderFlow['S_equilibrium'].count())
    ans.S_equilibrium_ratio = division_no_inf( ans.S_equilibrium, S_equilibrium->size());

    // res['Scjbs_equilibrium_ratio'] = replace_inf(res['Scjbs_equilibrium']/orderFlow['Scjbs_equilibrium'].count())
    ans.Scjbs_equilibrium_ratio = division_no_inf( ans.Scjbs_equilibrium, Scjbs_equilibrium->size());

    // // res['D_equilibrium_ratio'] = replace_inf(res['D_equilibrium']/orderFlow['D_equilibrium'].count())
    ans.D_equilibrium_ratio = division_no_inf( ans.D_equilibrium, D_equilibrium->size());
    
    // res['Dcjbs_equilibrium_ratio'] = replace_inf(res['Dcjbs_equilibrium']/orderFlow['Dcjbs_equilibrium'].count())
    ans.Dcjbs_equilibrium_ratio = division_no_inf( ans.Dcjbs_equilibrium, Dcjbs_equilibrium->size());

    // res['S_equilibrium_count'] = (orderFlow['S_equilibrium_count']>=3).sum()
    // res['Scjbs_equilibrium_count'] = (orderFlow['Scjbs_equilibrium_count']>=3).sum()
    // res['D_equilibrium_count'] = (orderFlow['D_equilibrium_count']>=3).sum()
    // res['Dcjbs_equilibrium_count'] = (orderFlow['Dcjbs_equilibrium_count']>=3).sum()
    ans.S_equilibrium_count = [&](){
        auto result = satisfy<uint32_t>(S_equilibrium_count,[](const uint32_t& v){
            return v >= 3; 
        });
        return simd_impl::sum(result);
    }();

    ans.Scjbs_equilibrium_count = [&](){
        auto result = satisfy<uint32_t>(Scjbs_equilibrium_count,[](const uint32_t& v){
            return v >= 3; 
        });
        return simd_impl::sum(result);
    }();

    ans.D_equilibrium_count = [&](){
        auto result = satisfy<uint32_t>(D_equilibrium_count,[](const uint32_t& v){
            return v >= 3; 
        });
        return simd_impl::sum(result);
    }();

    ans.Dcjbs_equilibrium_count = [&](){
        auto result = satisfy<uint32_t>(Dcjbs_equilibrium_count,[](const uint32_t& v){
            return v >= 3; 
        });
        return simd_impl::sum(result);
    }();

    // res['POC'] = orderFlow.loc[orderFlow.tradv == orderFlow['tradv'].max()]['tradp'].iloc[0]
    ans.POC = [&](){
        double tradv_max = simd_impl::max(tradv);
        auto result = satisfy<double>(tradv,[tradv_max](const double& v){
            return v == tradv_max; 
        });
        return selected_by<double>(tradp, result, simd_impl::sum(result))->front();
    }();
    // res['cjbsPOC'] = orderFlow.loc[orderFlow.cjbs == orderFlow['cjbs'].max()]['tradp'].iloc[0]
    ans.cjbsPOC = [&](){
        double cjbs_max = simd_impl::max(cjbs);
        auto result = satisfy<double>(cjbs,[cjbs_max](const double& v){
            return v == cjbs_max; 
        });
        return selected_by<double>(tradp, result, simd_impl::sum(result))->front();
    }();

    // res['SPOC'] = orderFlow.loc[orderFlow.Sv == orderFlow['Sv'].max()]['tradp'].iloc[0]
    ans.SPOC = [&](){
        double Sv_max = simd_impl::max(Sv);
        auto result = satisfy<double>(Sv,[Sv_max](const double& v){
            return v == Sv_max; 
        });
        return selected_by<double>(tradp, result, simd_impl::sum(result))->front();
    }();

    // res['ScjbsPOC'] = orderFlow.loc[orderFlow.Scjbs == orderFlow['Scjbs'].max()]['tradp'].iloc[0]
    ans.ScjbsPOC = [&](){
        double Scjbs_max = simd_impl::max(Scjbs);
        auto result = satisfy<double>(Scjbs,[Scjbs_max](const double& v){
            return v == Scjbs_max; 
        });
        return selected_by<double>(tradp, result, simd_impl::sum(result))->front();
    }();

    // res['BPOC'] = orderFlow.loc[orderFlow.Bv == orderFlow['Bv'].max()]['tradp'].iloc[0]
    ans.BPOC = [&](){
        double Bv_max = simd_impl::max(Bv);
        auto result = satisfy<double>(Bv,[Bv_max](const double& v){
            return v == Bv_max; 
        });
        return selected_by<double>(tradp, result, simd_impl::sum(result))->front();
    }();
    
    // res['BcjbsPOC'] = orderFlow.loc[orderFlow.Bcjbs == orderFlow['Bcjbs'].max()]['tradp'].iloc[0]
    ans.BcjbsPOC = [&](){
        double Bcjbs_max = simd_impl::max(Bcjbs);
        auto result = satisfy<double>(Bcjbs,[Bcjbs_max](const double& v){
            return v == Bcjbs_max; 
        });
        return selected_by<double>(tradp, result, simd_impl::sum(result))->front();
    }();
    
    // res['Svwap'] = replace_inf(res['Samt']/res['Sv'])
    // res['Bvwap'] = replace_inf(res['Bamt']/res['Bv'])
    ans.Svwap = division_no_inf(ans.Samt, ans.Sv);
    ans.Bvwap = division_no_inf(ans.Bamt, ans.Bv);
    
    // res['S_orderDecay'] = (orderFlow.groupby('datetime')['Sv'].first()/orderFlow.groupby('datetime')['Sv'].nth(2)).replace([np.inf,-np.inf],np.nan)
    // res['Scjbs_orderDecay'] = (orderFlow.groupby('datetime')['Scjbs'].first()/orderFlow.groupby('datetime')['Scjbs'].nth(2)).replace([np.inf,-np.inf],np.nan)
    if(Sv->size() < 2){
        ans.S_orderDecay = std::numeric_limits<double>::quiet_NaN();
    }else{
        ans.S_orderDecay = division_no_inf(Sv->front(), Sv->at(1));
    }
    if(Scjbs->size() < 2){
        ans.Scjbs_orderDecay = std::numeric_limits<double>::quiet_NaN();
    }else{
        ans.Scjbs_orderDecay = division_no_inf(Scjbs->front(), Scjbs->at(1));
    }

    // res['D_orderDecay'] = (orderFlow['Bv'].last()/orderFlow['Bv'].apply(lambda x: x.iloc[-2] if len(x)>1 else np.nan)).replace([np.inf,-np.inf],np.nan)
    // res['Dcjbs_orderDecay'] = (orderFlow.groupby('datetime')['Bcjbs'].last()/orderFlow.groupby('datetime')['Bcjbs'].apply(lambda x: x.iloc[-2] if len(x)>1 else np.nan)).replace([np.inf,-np.inf],np.nan)
    ans.D_orderDecay = [&](){
        double y = Bv->size() > 1 ? Bv->at( Bv->size() - 2) : std::numeric_limits<double>::quiet_NaN();
        return division_no_inf(Bv->back(),y);
    }();

    ans.Dcjbs_orderDecay = [&](){
        double y = Bcjbs->size() > 1 ? Bcjbs->at( Bcjbs->size() - 2) : std::numeric_limits<double>::quiet_NaN();
        return division_no_inf(Bcjbs->back(),y);
    }();
    // res['S_decaySlope'] = (orderFlow.groupby('datetime')['Sv'].apply(lambda x: x.corr(pd.Series(np.arange(len(x)),index=x.index))*x.std()/pd.Series(np.arange(len(x)),index=x.index).std())).replace([np.inf,-np.inf],np.nan)
    // res['Scjbs_decaySlope'] = (orderFlow.groupby('datetime')['Scjbs'].apply(lambda x: x.corr(pd.Series(np.arange(len(x)),index=x.index))*x.std()/pd.Series(np.arange(len(x)),index=x.index).std())).replace([np.inf,-np.inf],np.nan)
    ans.S_decaySlope = [&](){
        std::vector<double> vec(Sv->size());
        std::iota(vec.begin(), vec.end(), 0.0);
        auto base_ptr = std::make_unique<std::vector<double>>(std::move(vec));
        double a = simd_impl::corr(Sv, base_ptr);
        double b = simd_impl::std(Sv, simd_impl::mean(Sv));
        double c = simd_impl::std(base_ptr, simd_impl::mean(base_ptr));
        return division_no_inf(a*b,c);
    }();

    ans.Scjbs_decaySlope = [&](){
        std::vector<double> vec(Scjbs->size());
        std::iota(vec.begin(), vec.end(), 0.0);
        auto base_ptr = std::make_unique<std::vector<double>>(std::move(vec));
        double x = simd_impl::corr(Scjbs, base_ptr) * simd_impl::std(Scjbs, simd_impl::mean(Scjbs));
        double y = simd_impl::std(base_ptr, simd_impl::mean(base_ptr));
        return division_no_inf(x,y);
    }();

    // res['D_decaySlope'] = (orderFlow.groupby('datetime')['Bv'].apply(lambda x: x.corr(pd.Series(np.arange(len(x)),index=x.index))*x.std()/pd.Series(np.arange(len(x)),index=x.index).std())).replace([np.inf,-np.inf],np.nan)
    ans.D_decaySlope = [&](){
        std::vector<double> vec(Bv->size());
        std::iota(vec.begin(), vec.end(), 0.0);
        auto base_ptr = std::make_unique<std::vector<double>>(std::move(vec));
        double x = simd_impl::corr(Bv, base_ptr) * simd_impl::std(Bv, simd_impl::mean(Bv));
        double y = simd_impl::std(base_ptr, simd_impl::mean(base_ptr));
        return division_no_inf(x,y);
    }();
    
    // res['S_decaySlope'] = (orderFlow.groupby('datetime')['Bcjbs'].apply(lambda x: x.corr(pd.Series(np.arange(len(x)),index=x.index))*x.std()/pd.Series(np.arange(len(x)),index=x.index).std())).replace([np.inf,-np.inf],np.nan)
    ans.Dcjbs_decaySlope = [&](){
        std::vector<double> vec(Bcjbs->size());
        std::iota(vec.begin(), vec.end(), 0.0);
        auto base_ptr = std::make_unique<std::vector<double>>(std::move(vec));
        double x = simd_impl::corr(Bcjbs, base_ptr) * simd_impl::std(Bcjbs, simd_impl::mean(Bcjbs));
        double y = simd_impl::std(base_ptr, simd_impl::mean(base_ptr));
        return division_no_inf(x,y);
    }();

    return ans.to_vector();
}
#endif