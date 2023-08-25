
#ifndef TABLE_DESCRIPTOR
#define TABLE_DESCRIPTOR

#include <bits/stdc++.h>
#include <fmt/format.h>
#include <absl/log/absl_log.h>
#include <absl/log/log.h>
#include <time.h>

using buf = std::pair<uint8_t*,size_t>;
class TableDescriptor{
public:
  std::vector<buf> bufs;
  std::vector<std::string> col_types;
  std::vector<std::string> col_names;
  /**
    * @brief 由于必须考虑NaN的情况，因此需要构建一个位图索引
    * 
    */
  std::vector<std::vector<bool>> is_Nulls;
  std::vector<uint8_t*> is_Nulls_buffer;
  /**
   * @brief 存在两种情况
   * [1]string以外 fixed_byte_array, double, int64等，都有固定的长度 ele_size[index][0]来进行访问
   * [2]特别的，考虑string，由于长度不对齐，因此需要加一级索引 ele_size[index][i]来进行访问
   * 在此，不打算支持这种情况，全部视为定长
  */
  std::vector<uint32_t> ele_size;

  std::string status(){
    std::stringstream ss;
    ss << "\nCols info:\n";
    for(int i = 0 ; i < col_types.size(); i++){
      ss << fmt::format("{}-{}({}) ",col_names[i],col_types[i],ele_size[i]);
    }
    ss << std::endl;
    return ss.str();
  }
  /**
   * @brief TableDescriptor的使用场景包括:
   * [1]从parquet中读取数据，此时需要分配动态内存
   * [2]从DataFrame中读取数据，等价于一个对DataFrame的snapshot
   */
  void release(){
    for(int i = 0; i < bufs.size(); i++){
      delete bufs[i].first;
      if(is_Nulls_buffer[i] != nullptr){
        delete is_Nulls_buffer[i];
      }
    } 
    return;
  }
  ~TableDescriptor(){
    release();
    DLOG(INFO) << "TableDescriptor destory";
  }
};

#endif