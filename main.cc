#include "src/collect_layer/collector.hpp"

//单元测试
//collect模块

//从publish.json中读取并计算
//1.需要启用的计算函数
//2.计算的最小周期
//3.需要执行计算的周期
void test_PeriodCtxConfig(){
    PeriodCtxConfig trade_ctx_config(CtxType::TRADE);
    trade_ctx_config.init();
}

int main()
{

    return 0;
}