#ifndef TRADE_PARSER_HPP
#define TRADE_PARSER_HPP
#include "parser.hpp"
class SZTradeInfoParser : public Parser<TradeInfo,EXCHANGE::SZ>{
public:
    absl::Duration filter_boundary;
    explicit SZTradeInfoParser(const std::shared_ptr<InfoCache<TradeInfo,EXCHANGE::SZ>>& ptr ):Parser<TradeInfo,EXCHANGE::SZ>(ptr){

                                                                                                 };
public:
    void MessageProcess(std::string_view line) override{
        std::vector<std::string_view> x = absl::StrSplit(line, ",");
        std::shared_ptr<TradeInfo> TickInfo_sp = std::make_shared<TradeInfo>();
        int ExecType;
        CHECK_RETURN_VALUE(absl::SimpleAtoi(x[9], &ExecType),"Failed to convert ExecType");
        if(ExecType == 70){
            absl::Duration bias = convert_time_string_to_duration(x[11]);
            if(bias < filter_boundary){
                return;
            }
            std::shared_ptr<TradeInfo> ticker_info_sp = std::make_shared<TradeInfo>();
            ticker_info_sp->SecurityID = stringViewToArray<11>(x[5]);
            set_trade_suffix<TradeInfo,EXCHANGE::SZ>(ticker_info_sp->SecurityID);
            CHECK_RETURN_VALUE(absl::SimpleAtod(x[7],&ticker_info_sp->TradPrice),"Failed to convert TradPrice");
            CHECK_RETURN_VALUE(absl::SimpleAtod(x[8], &ticker_info_sp->TradVolume),"Failed to convert TradVolume");
            ticker_info_sp->TradTime = InfoCache_ptr->today_start + bias;
            int64_t BidApplSeqNum;
            int64_t OfferApplSeqNum;
            CHECK_RETURN_VALUE(absl::SimpleAtoi(x[3],&BidApplSeqNum),"Failed to convert BidApplSeqNum");
            CHECK_RETURN_VALUE(absl::SimpleAtoi(x[4],&OfferApplSeqNum),"Failed to convert OfferApplSeqNum");
            ticker_info_sp->B_or_S = (BidApplSeqNum > OfferApplSeqNum) ? 1 : -1;
            InfoCache_ptr->put_info(ticker_info_sp);
        }
    }
};
//上交所逐笔成交
class SHTradeInfoParser: public Parser<TradeInfo,EXCHANGE::SH>{
public:
    explicit SHTradeInfoParser(const std::shared_ptr<InfoCache<TradeInfo,EXCHANGE::SH>>& ptr ): Parser<TradeInfo,EXCHANGE::SH>(ptr){};
public:
    void MessageProcess(std::string_view line) override {
        std::vector<std::string_view> x = absl::StrSplit(line,",");
        if(x[10] != "N"){
            absl::Duration bias = convert_time_string_to_duration(x[4]);
            if(bias < filter_boundary){
                return;
            }
            std::shared_ptr<TradeInfo> TickInfo_sp = std::make_shared<TradeInfo>();
            // CHECK_RETURN_VALUE(absl::SimpleAtoi(x[3], &ticker_info_sp->SecurityID),"Failed to convert SecurityID");
            TickInfo_sp->SecurityID = stringViewToArray<11>(x[3]);
            set_trade_suffix<TradeInfo,EXCHANGE::SH>(TickInfo_sp->SecurityID);
            CHECK_RETURN_VALUE(absl::SimpleAtod(x[5],&TickInfo_sp->TradPrice),"Failed to convert TradPrice");
            CHECK_RETURN_VALUE(absl::SimpleAtod(x[6], &TickInfo_sp->TradVolume),"Failed to convert TradVolume");
            TickInfo_sp->TradTime = InfoCache_ptr->today_start + bias;
            TickInfo_sp->B_or_S = (x[10] == "B") ? 1 : -1;
            InfoCache_ptr->put_info(TickInfo_sp);
        }
    }
};
#endif