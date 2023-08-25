#ifndef __PARQUET_IO_HPP__
#define __PARQUET_IO_HPP__

#include "table_descriptor.hpp"
#include <arrow/csv/api.h>
#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/util/checked_cast.h>
#include <parquet/arrow/writer.h>
#include <arrow/util/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/filesystem/api.h>

std::string uint8_to_binary_string(uint8_t* array, size_t size) {
    std::ostringstream oss;

    for(size_t i = 0; i < size; ++i) {
        oss << std::bitset<8>(array[i]);
    }
    return oss.str();
}

std::shared_ptr<arrow::Buffer> serialize_to_arrow_ipc(std::shared_ptr<TableDescriptor> td_sp) {
    arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    int col_count = td_sp->bufs.size();
    std::vector<std::shared_ptr<arrow::Field>> fields = std::vector<std::shared_ptr<arrow::Field>>(col_count);
    std::vector<std::shared_ptr<arrow::Array>> cols;
    for(int i = 0; i < col_count; i++){
      std::string col_name = td_sp->col_names[i];
      std::string col_type = td_sp->col_types[i];
      if(col_type == "_fixed_str"){
        fields[i] = arrow::field(col_name, arrow::fixed_size_binary(td_sp->ele_size[i]));
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto array_data = arrow::ArrayData::Make(arrow::fixed_size_binary(td_sp->ele_size[i]),td_sp->bufs[i].second / td_sp->ele_size[i],{nullptr, buffer});
        auto array = arrow::MakeArray(array_data);
        cols.push_back(std::move(array));
      }
      else if(col_type == "_uint64"){
        fields[i] = arrow::field(col_name, arrow::uint64());
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto array_data = arrow::ArrayData::Make(arrow::uint64(),td_sp->bufs[i].second / td_sp->ele_size[i] , {nullptr, buffer});
        auto array = arrow::MakeArray(array_data);
        cols.push_back(std::move(array));
      }
      else if(col_type == "_int64"){
        fields[i] = arrow::field(col_name, arrow::int64());
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto buffer_na = arrow::Buffer::Wrap(td_sp->is_Nulls_buffer[i], static_cast<int>(std::ceil(td_sp->bufs[i].second / td_sp->ele_size[i] / 8.0)));
        auto array_data = arrow::ArrayData::Make(arrow::int64(),td_sp->bufs[i].second / td_sp->ele_size[i],{buffer_na, buffer});
        auto array = arrow::MakeArray(array_data);
        cols.push_back(std::move(array));
      }
      else if(col_type == "_double"){
        fields[i] = arrow::field(col_name, arrow::float64());
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto buffer_na = arrow::Buffer::Wrap(td_sp->is_Nulls_buffer[i], static_cast<int>(std::ceil(td_sp->bufs[i].second / td_sp->ele_size[i] / 8.0)));
        auto array_data = arrow::ArrayData::Make(arrow::float64(),td_sp->bufs[i].second / td_sp->ele_size[i],{buffer_na, buffer});
        auto array = arrow::MakeArray(array_data);
        cols.push_back(std::move(array));

      }      
      else if(col_type == "_timestramp"){
        fields[i] = arrow::field(col_name, arrow::timestamp(arrow::TimeUnit::MICRO));
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto buffer_na = arrow::Buffer::Wrap(td_sp->is_Nulls_buffer[i], static_cast<int>(std::ceil(td_sp->bufs[i].second / td_sp->ele_size[i] / 8.0)));
        auto array_data = arrow::ArrayData::Make(arrow::timestamp(arrow::TimeUnit::MICRO),td_sp->bufs[i].second / td_sp->ele_size[i],{buffer_na, buffer});
        auto array = arrow::MakeArray(array_data);
        cols.push_back(std::move(array));
      }
    }
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, cols[0]->length(), cols);
    auto out = arrow::io::BufferOutputStream::Create(0, arrow::default_memory_pool()).ValueOrDie();
    // 创建一个RecordBatchWriter，写入一个完整的Arrow IPC流
    auto writer = arrow::ipc::MakeStreamWriter(out, batch->schema()).ValueOrDie();
    st = writer->WriteRecordBatch(*batch);
    st = writer->Close();
    // 获取BufferOutputStream中的Buffer
    return out->Finish().ValueOrDie();
}

/**
 * @brief 基于TableDescriptor的数据引用，不会产生额外的数据复制，来构造table
 * 
 * @param path 
 * @param td_sp 
 * @return arrow::Status 
 */
arrow::Status persist_to_parquet(std::string path,std::shared_ptr<TableDescriptor> td_sp) {
    arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    int col_count = td_sp->bufs.size();
    std::vector<std::shared_ptr<arrow::Field>> fields = std::vector<std::shared_ptr<arrow::Field>>(col_count);
    std::vector<std::shared_ptr<arrow::ChunkedArray>> cols;
    for(int i = 0; i < col_count; i++){
      std::string col_name = td_sp->col_names[i];
      std::string col_type = td_sp->col_types[i];
      if(col_type == "_fixed_str"){
        fields[i] = arrow::field(col_name, arrow::fixed_size_binary(td_sp->ele_size[i]));
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto array_data = arrow::ArrayData::Make(arrow::fixed_size_binary(td_sp->ele_size[i]),td_sp->bufs[i].second / td_sp->ele_size[i],{nullptr, buffer});
        auto array = arrow::MakeArray(array_data);
        auto col = std::make_shared<arrow::ChunkedArray>(std::move(array));
        cols.push_back(std::move(col));
      }
      else if(col_type == "_uint64"){
        fields[i] = arrow::field(col_name, arrow::uint64());
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto array_data = arrow::ArrayData::Make(arrow::uint64(),td_sp->bufs[i].second / td_sp->ele_size[i] , {nullptr, buffer});
        auto array = arrow::MakeArray(array_data);
        auto col = std::make_shared<arrow::ChunkedArray>(std::move(array));
        cols.push_back(std::move(col));
      }
      else if(col_type == "_int64"){
        fields[i] = arrow::field(col_name, arrow::int64());
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto buffer_na = arrow::Buffer::Wrap(td_sp->is_Nulls_buffer[i], static_cast<int>(std::ceil(td_sp->bufs[i].second / td_sp->ele_size[i] / 8.0)));
        auto array_data = arrow::ArrayData::Make(arrow::int64(),td_sp->bufs[i].second / td_sp->ele_size[i],{buffer_na, buffer});
        auto array = arrow::MakeArray(array_data);
        auto col = std::make_shared<arrow::ChunkedArray>(std::move(array));
        cols.push_back(std::move(col));
      }
      else if(col_type == "_double"){
        fields[i] = arrow::field(col_name, arrow::float64());
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto buffer_na = arrow::Buffer::Wrap(td_sp->is_Nulls_buffer[i], static_cast<int>(std::ceil(td_sp->bufs[i].second / td_sp->ele_size[i] / 8.0)));
        auto array_data = arrow::ArrayData::Make(arrow::float64(),td_sp->bufs[i].second / td_sp->ele_size[i],{buffer_na, buffer});
        auto array = arrow::MakeArray(array_data);
        auto col = std::make_shared<arrow::ChunkedArray>(std::move(array));
        cols.push_back(std::move(col));

      }      
      else if(col_type == "_timestramp"){
        fields[i] = arrow::field(col_name, arrow::timestamp(arrow::TimeUnit::MICRO));
        auto buffer = arrow::Buffer::Wrap(td_sp->bufs[i].first, td_sp->bufs[i].second);
        auto buffer_na = arrow::Buffer::Wrap(td_sp->is_Nulls_buffer[i], static_cast<int>(std::ceil(td_sp->bufs[i].second / td_sp->ele_size[i] / 8.0)));
        auto array_data = arrow::ArrayData::Make(arrow::timestamp(arrow::TimeUnit::MICRO),td_sp->bufs[i].second / td_sp->ele_size[i],{buffer_na, buffer});
        auto array = arrow::MakeArray(array_data);
        auto col = std::make_shared<arrow::ChunkedArray>(std::move(array));
        cols.push_back(std::move(col));
      }
    }
      std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
      auto table = arrow::Table::Make(schema,cols);
      using parquet::ArrowWriterProperties;
      using parquet::WriterProperties;
      std::shared_ptr<WriterProperties> props = WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
      std::shared_ptr<ArrowWriterProperties> arrow_props = ArrowWriterProperties::Builder().store_schema()->build();
      std::shared_ptr<arrow::io::FileOutputStream> outfile;
      ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(path));
      try {
          PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, /*chunk_size=*/64 * 1024 * 1024, props, arrow_props)); 
      } catch (const std::exception& e) {
          std::cerr << "Caught exception: " << e.what() << std::endl;
      }
      return arrow::Status::OK();
}


arrow::Status init_from_parquet(std::string path,std::shared_ptr<TableDescriptor> tabled_sp){
      arrow::MemoryPool* pool = arrow::default_memory_pool();
      auto reader_properties = parquet::ReaderProperties(pool);
      reader_properties.set_buffer_size(4096 * 4);
      reader_properties.enable_buffered_stream();
      auto arrow_reader_props = parquet::ArrowReaderProperties();
      
      parquet::arrow::FileReaderBuilder reader_builder;
      ARROW_RETURN_NOT_OK(
      reader_builder.OpenFile(path, /*memory_map=*/false, reader_properties));
      reader_builder.memory_pool(pool);
      reader_builder.properties(arrow_reader_props);
      std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
      ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());
      std::shared_ptr<arrow::Table> table;    
      ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

      auto fields = table->fields();
      int rows = table->num_rows();
      auto cols = table->columns();
      tabled_sp->col_names = table->ColumnNames();
      for(int k = 0; k < cols.size(); k++){
          auto chks = cols[k]->chunks();
          auto chk = chks[0];
          std::string col_type = chk->data()->type->ToString();
          if(col_type.rfind("fixed_size_binary", 0) == 0){
              size_t length = chk->data()->buffers[1]->size();
              uint8_t* membuf = new uint8_t[length];
              auto ptr = chk->data()->buffers[1]->address();         
              std::memcpy(membuf, reinterpret_cast<uint8_t*>(ptr),length);
              tabled_sp->ele_size.emplace_back(length/rows);
              tabled_sp->bufs.emplace_back(std::pair(membuf,length));
              tabled_sp->col_types.emplace_back("_fixed_str");
              //the fixed_size_binary is fixed_length_str, should never be NaN, so is_Nulls will be all 1 represent have value
              tabled_sp->is_Nulls.emplace_back(rows,1);
              //不确实是否应该计算
              tabled_sp->is_Nulls_buffer.push_back(nullptr);
          }
        else if(col_type == "string"){
          size_t length = chk->data()->buffers[2]->size();
          /**
           * @brief 对于string没有NaN的概念
          */
          /**
           * @brief string本来有长度计算的部分，由于只考虑定长，因此可以忽略
           * 实际的表示为，buf[1]表示了每个元素的offset信息  0|12|24|36|48|60|72|84|96|108| (12)
           * 需要注意的是，offset的保存方式是uint32，但实际上有64位对齐
           * 
           * size_t string_length = chk->data()->buffers[1]->size();
           * auto string_length_ptr = chk->data()->buffers[1]->address();
           * reinterpret_cast<unsigned int*>(string_length_ptr)[i]
          */
          auto ptr = chk->data()->buffers[2]->address();
          uint8_t* membuf = new uint8_t[length];         
          std::memcpy(membuf, reinterpret_cast<uint8_t*>(ptr),length);
          tabled_sp->ele_size.emplace_back(length/rows);
          tabled_sp->bufs.emplace_back(std::pair(membuf,length));
          tabled_sp->col_types.emplace_back("_fixed_str");
          tabled_sp->is_Nulls.emplace_back(rows,1);
          tabled_sp->is_Nulls_buffer.push_back(nullptr);
        }
        else if(col_type == "uint64"){
          size_t length = chk->data()->buffers[1]->size();
          auto ptr = chk->data()->buffers[1]->address();
          // std::cout<<(chk->data()->buffers[0] == nullptr)<< std::endl;
          uint8_t* membuf = new uint8_t[length];         
          std::memcpy(membuf, reinterpret_cast<uint8_t*>(ptr),length);
          tabled_sp->ele_size.emplace_back(length/rows);
          tabled_sp->bufs.emplace_back(membuf,length);
          tabled_sp->col_types.emplace_back(fmt::format("_{}",col_type));
          tabled_sp->is_Nulls.emplace_back(rows,1);
          tabled_sp->is_Nulls_buffer.push_back(nullptr);
        }
        else if(col_type == "double" || col_type == "int64"){
          size_t length = chk->data()->buffers[1]->size();
          /**
           * @brief 检查Null部分
           * parquet中对Is_Null的存储方式是uint8(char)的二进制bitmap，因此存在对齐的问题，需要先计算出size
           * 先赋值，然后裁剪掉padding的部分使用
          */
          size_t length_null = chk->data()->buffers[0]->size();
          std::vector<bool> Is_Null(length_null * 8);
          auto bitmap_ptr = reinterpret_cast<uint8_t*>(chk->data()->buffers[0]->address()); 
          for(int i = 0; i < length_null; i++){
              std::bitset<8> bin_8(bitmap_ptr[i]);
              for(int j = 0; j < 8; j++){
                Is_Null[i*8 + j] = bin_8[j]; 
              }
          }
          Is_Null.resize(rows);
          uint8_t* Null_place = new uint8_t[length_null];
          std::memcpy(Null_place,bitmap_ptr,length_null);
          /**
           * @brief 获取值的部分
          */
          auto ptr = chk->data()->buffers[1]->address();
          uint8_t* membuf = new uint8_t[length];         
          std::memcpy(membuf, reinterpret_cast<uint8_t*>(ptr),length);
          tabled_sp->ele_size.emplace_back(length/rows);
          tabled_sp->bufs.emplace_back(membuf,length);
          //int64->_int64, double->_double, uint64->_uint64
          tabled_sp->col_types.emplace_back(fmt::format("_{}",col_type));
          tabled_sp->is_Nulls.push_back(std::move(Is_Null));
          tabled_sp->is_Nulls_buffer.push_back(Null_place);
        }
        else if(col_type == "timestamp[us]"){
          size_t length = chk->data()->buffers[1]->size();
          //Null check
          size_t length_null = chk->data()->buffers[0]->size();
          std::vector<bool> Is_Null(length_null * 8);
          auto bitmap_ptr = reinterpret_cast<uint8_t*>(chk->data()->buffers[0]->address()); 
          for(int i = 0; i < length_null; i++){
              std::bitset<8> bin_8(bitmap_ptr[i]);
              for(int j = 0; j < 8; j++){
                Is_Null[i*8 + j] = bin_8[j]; 
              }
          }
          Is_Null.resize(rows);
          uint8_t* Null_place = new uint8_t[length_null];
          std::memcpy(Null_place,bitmap_ptr,length_null);
          auto ptr = chk->data()->buffers[1]->address();
          uint8_t* membuf = new uint8_t[length];         
          std::memcpy(membuf, reinterpret_cast<uint8_t*>(ptr),length);
          tabled_sp->ele_size.emplace_back(length/rows);
          tabled_sp->bufs.emplace_back(std::pair(membuf,length));
          tabled_sp->col_types.emplace_back("_timestamp");
          tabled_sp->is_Nulls.push_back(std::move(Is_Null));
          tabled_sp->is_Nulls_buffer.push_back(Null_place);
        }
        else{
          LOG(FATAL) << "Unsupport parquet type";
        }
      }
    
    return arrow::Status::OK();
};

#endif