 #include "common/utils.hpp"
 #include "cache_layer/info_cache_manager.hpp"
 #include "logger/async_logger.hpp"
 #include "collect_layer/trade_collector.hpp"
 class ConfigMaker{
 public:
     static void prepare();
 };

 void ConfigMaker::prepare(){
     if(!std::filesystem::exists("config")){
         throw std::runtime_error("config folder not find");
     }
     json config = open_json_file("config/system.json");
     //嵌入python脚本，获取当前的天数, 需要track的股票数，和前一天的收盘价closeprice
     std::string hfq_table_path = config["system"]["hfq_multi_parquet_path"];
     std::string today_str = config["system"]["date"];
     std::system(fmt::format("python3 script.py {} {}", hfq_table_path,today_str).c_str());
     //生成调用函数表
 }

 int main()
 {
     ConfigMaker::prepare();
     auto logger_manager_sp =  std::make_shared<LoggerManager>();
     //此时，aggregator_manager仅仅构造，但不实质启动，因为具体需要缓存表格，依赖于j
     auto aggregator_manager_sp = std::make_shared<AggregatorManager>("parquet_result");

     //先配置依赖最简单的TradeInfoCacheManager
     auto trade_info_cache_manager = std::make_shared<TradeInfoCacheManager>(logger_manager_sp);
     //Cache层的构造逻辑如下:
     //1.构造TradeInfoCacheManager自身，注册日志模块,在构造函数中完成
     //2.根据system.json 中的 cache_size 参数，对缓存层进行初始化，此处是一个链式的构造

     //对于上交所和深交所，会调用TradeCache类，即Cache基本结构
     //a.根据参数cache_size 默认为8192 因为会被快速的搬运分发到各个合约所属的buffer中 对应数据为info_queue_ptr
     //b.today_start 即EMDS的运行日期，会在parser中用到

     //3.调用parser构造函数，启动parser线程，监听发送端口的推送消息，解析后推入queue中
     //4.调用init_contract_buffer_map，构造余下的buffer
     //5.调用run_deliver_threads，启动线程池，将消息下发，从queue中搬运走
     trade_info_cache_manager->run_deliver_threads(4);

     //此时，整个cache层已经完全启动，可以正常的接收信息，并分发

     //Collector层的构造逻辑如下:
     auto trade_collector_sp = std::make_shared<TradeCollectorManager>(aggregator_manager_sp);
     trade_collector_sp->register_collect_target_buffer_map(
         trade_info_cache_manager->sh_trade_info_cache->security_id_to_buffer_map_sp,
         trade_info_cache_manager->sz_trade_info_cache->security_id_to_buffer_map_sp
     );
     trade_collector_sp->init(logger_manager_sp);
     //1.TradeCollectorManager 继承类 CollectorManager<TradeInfo>，首先调用父类构造函数，注册日志发生器
     //2.注册需要收集的对象，即Cache层的最终缓存
     //3.先调用父类的virtual init
     //4.构建自己的collector collector
     //调用init，从配置初始化信息
     //a.读取交易日的开始时间
     //b.读取收集器的延迟时间
     //c.读取收集器需要收集的股票 数据维护了太多副本，没有必要，后面抽出来
    
     //a.ContractBufferMapCollector<T,EX> ---- | 收集数据 | ---- ContractBufferMap<T,EX> 绑定Map
     //b.ContractBufferMapCollector<T,EX> ---- | 处理计算 | ---- ContractPeriodComputer<T, EX> 绑定计算器
     aggregator_manager_sp->init("config","trade");
     return 0;
 }
