#ifndef CONTRACT_COLLECTOR_MANAGER_HPP
#define CONTRACT_COLLECTOR_MANAGER_HPP
#include <memory>
#include "../common/info.hpp"
#include <thread>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include "../common/time_utils.hpp"
#include "../common/fmt_expand.hpp"
#include "../common/utils.hpp"
#include "../common/config_type.hpp"
#include "collector.hpp"

class TradeCollectorManager{
public:
    using TradeCollector = ContractBufferMapCollector<TradeInfo>;
    std::shared_ptr<TradeCollector> sz_trade_collector;
    std::shared_ptr<TradeCollector> sh_trade_collector;
};

#endif