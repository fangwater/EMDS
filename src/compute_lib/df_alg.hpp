#ifndef DF_ALG_H
#define DF_ALG_H

#include <bits/stdc++.h>
#include <immintrin.h>
#include <iostream>
#include <memory>
#include <vector>
#include <xsimd/xsimd.hpp>
#include <stdexcept>
#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>
#include <type_traits>

double division_no_inf(double x, double y) {
    if (y == 0.0) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    double res = x / y;
    if (std::isinf(res)) {
        return std::numeric_limits<double>::quiet_NaN();
    } else {
        return res;
    }
}

template<typename T>
concept TriviallyCopyable = std::is_trivially_copyable_v<T>;



template<typename T>
std::unique_ptr<std::vector<T>> cast_to_double(std::unique_ptr<std::vector<T>>& base_ptr){
    auto res_ptr = std::make_unique<std::vector<double>>(base_ptr->size());
    for(int i = 0; i < res_ptr->size(); i++){
        res_ptr->at(i) = base_ptr->at(i);
    }
    return res_ptr;
}


template<typename T>
std::unique_ptr<std::vector<T>> shift(std::unique_ptr<std::vector<T>>& base_ptr, int32_t steps, T cval) {
    if(base_ptr->size() == 1){
        auto res_ptr = std::make_unique<std::vector<T>>();
        res_ptr->push_back(cval);
        return res_ptr;
    }
    auto res_ptr = std::make_unique<std::vector<T>>(base_ptr->size());
    if(steps % base_ptr->size() == 0){
        std::memcpy(res_ptr->data(),base_ptr->data(),sizeof(T)*base_ptr->size());
        return res_ptr;
    }else if(steps < 0){
        steps = -steps;
        steps = steps % base_ptr->size();
        std::memcpy(res_ptr->data(), base_ptr->data() + steps, sizeof(T) * (base_ptr->size() - steps));
        std::fill_n(res_ptr->begin() + base_ptr->size() - steps, steps , cval);
        return res_ptr;
    }
    for (size_t i = 0; i < base_ptr->size(); ++i) {
        size_t new_index = (i + steps) % base_ptr->size();
        res_ptr->at(new_index) = base_ptr->at(i);
    }

    for (size_t i = 0; i < steps; ++i) {
        res_ptr->at(i) = cval;
    }
    return res_ptr;
}

template <typename T>
std::unique_ptr<std::vector<T>> consecutive_sum(const std::unique_ptr<std::vector<T>>& input) {
    auto result = std::make_unique<std::vector<T>>(input->size());
    T prev_val = input->at(0);
    T count = 1;
    result->at(0) = prev_val;
    for (size_t i = 1; i < input->size(); ++i) {
        if (input->at(i) == prev_val) {
            count++;
        } else {
            prev_val = input->at(i);
            count = 1;
        }
        result->at(i) = count * input->at(i);
    }

    return result;
}

void replace_inf_with_nan(std::unique_ptr<std::vector<double>>& base_ptr){
    for(int i = 0; i < base_ptr->size(); i++){
        if(std::isinf(base_ptr->at(i))){
            base_ptr->at(i) = std::numeric_limits<double>::quiet_NaN();
        }
    }
}

void replace_nan_with_zero(std::unique_ptr<std::vector<double>>& base_ptr){
    for(int i = 0; i < base_ptr->size(); i++){
        if(std::isnan(base_ptr->at(i))){
            base_ptr->at(i) = 0;
        }
    }
}

namespace concat{
    template<TriviallyCopyable T>
    std::unique_ptr<std::vector<T>> direct(std::unique_ptr<std::vector<T>>& vec1_ptr, std::unique_ptr<std::vector<T>>& vec2_ptr) {
        size_t size1 = vec1_ptr->size();
        size_t size2 = vec2_ptr->size();
        std::unique_ptr<std::vector<T>> merged_vec_ptr = std::make_unique<std::vector<T>>(size1 + size2);

        std::memcpy(merged_vec_ptr->data(), vec1_ptr->data(), size1 * sizeof(T));
        std::memcpy(merged_vec_ptr->data() + size1, vec2_ptr->data(), size2 * sizeof(T));

        return merged_vec_ptr;
    }
    template<typename T>
    std::unique_ptr<std::vector<T>> direct_copy(const std::unique_ptr<std::vector<T>>& vec1_ptr, const std::unique_ptr<std::vector<T>>& vec2_ptr) {
        size_t size1 = vec1_ptr->size();
        size_t size2 = vec2_ptr->size();
        std::unique_ptr<std::vector<T>> merged_vec_ptr = std::make_unique<std::vector<T>>(size1 + size2);

        std::copy(vec1_ptr->begin(), vec1_ptr->end(), merged_vec_ptr->begin());
        std::copy(vec2_ptr->begin(), vec2_ptr->end(), merged_vec_ptr->begin() + size1);

        return merged_vec_ptr;
    }


    template<typename T>
    auto horizontal(std::unique_ptr<std::vector<T>>& vec1_ptr, std::unique_ptr<std::vector<T>>& vec2_ptr){
        std::unique_ptr<std::vector<T>> merged_vec_ptr = std::make_unique<std::vector<T>>();
        std::vector<uint32_t> vec1_mapping(vec1_ptr->size());
        std::vector<uint32_t> vec2_mapping(vec2_ptr->size());
        size_t index1 = 0, index2 = 0;
        while (index1 < vec1_ptr->size() || index2 < vec2_ptr->size()) {
            T value;
            if (index2 >= vec2_ptr->size() || (index1 < vec1_ptr->size() && vec1_ptr->at(index1) < vec2_ptr->at(index2))) {
                value = vec1_ptr->at(index1);
                vec1_mapping[index1++] = merged_vec_ptr->size();
            } else if (index1 >= vec1_ptr->size() || (index2 < vec2_ptr->size() && vec1_ptr->at(index1) > vec2_ptr->at(index2))) {
                value = vec2_ptr->at(index2);
                vec2_mapping[index2++] = merged_vec_ptr->size();
            } else {
                value = vec1_ptr->at(index1);
                vec1_mapping[index1++] = merged_vec_ptr->size();
                vec2_mapping[index2++] = merged_vec_ptr->size();
            }
            merged_vec_ptr->push_back(value);
        }
        return std::make_tuple(std::move(merged_vec_ptr), std::move(std::make_unique<std::vector<uint32_t>>( std::move(vec1_mapping) )), std::move(std::make_unique<std::vector<uint32_t>>( std::move(vec2_mapping) )));

    }

    template<typename T>
    requires(std::is_same_v<T,double>)
    std::unique_ptr<std::vector<T>> apply(std::unique_ptr<std::vector<uint32_t>>& mapping_vec_ptr, uint64_t merged_vec_size, std::unique_ptr<std::vector<T>>& base_ptr, std::string fillnan_method){
        std::unique_ptr<std::vector<T>> res_ptr;
        if( fillnan_method == "zero"){
            res_ptr = std::make_unique<std::vector<T>>( std::move(std::vector<T>(merged_vec_size, 0)) );
        }else if(fillnan_method == "none"){
            res_ptr = std::make_unique<std::vector<T>>( std::move(std::vector<T>(merged_vec_size, std::numeric_limits<T>::quiet_NaN())) );
        }else{
            exit(0);
        }
        for(int i = 0; i < mapping_vec_ptr->size(); i++){
            res_ptr->at(mapping_vec_ptr->at(i)) = base_ptr->at(i);
        }
        return std::move( res_ptr );
    };

    template<typename T>
    requires(std::is_same_v<T,int64_t>)
    std::unique_ptr<std::vector<T>> apply(std::unique_ptr<std::vector<uint32_t>>& mapping_vec_ptr, uint64_t merged_vec_size, std::unique_ptr<std::vector<T>>& base_ptr, std::string fillnan_method){
        std::unique_ptr<std::vector<T>> res_ptr;
        if( fillnan_method == "zero"){
            res_ptr = std::make_unique<std::vector<T>>( std::move(std::vector<T>(merged_vec_size, 0)) );
        }else if(fillnan_method == "none"){
            res_ptr = std::make_unique<std::vector<T>>( std::move(std::vector<T>(merged_vec_size, std::numeric_limits<int64_t>::min())) );
        }else{
            exit(0);
        }
        for(int i = 0; i < mapping_vec_ptr->size(); i++){
            res_ptr->at(mapping_vec_ptr->at(i)) = base_ptr->at(i);
        }
        return std::move( res_ptr );
    };

}

double quantile_mod(std::unique_ptr<std::vector<double>>& data_ptr, double percentile) {
    if (data_ptr->empty() || percentile <= 0 || percentile >= 1) {
        return 0.0;
    }
    double index = data_ptr->size() * percentile;
    int lower_index = static_cast<int>(index + 0.000000001);
    int upper_index = static_cast<int>(std::ceil(index - 0.000000001));
    if (lower_index == upper_index == index) {
        std::nth_element(data_ptr->begin(), data_ptr->begin() + lower_index, data_ptr->end());
        return data_ptr->at(lower_index-1);
    }
    std::nth_element(data_ptr->begin(), data_ptr->begin() + lower_index - 1, data_ptr->end());
    double lower_value = data_ptr->at(lower_index - 1);
    std::nth_element(data_ptr->begin(), data_ptr->begin() + upper_index - 1, data_ptr->end());
    double upper_value = data_ptr->at(upper_index - 1);
    double result = lower_value + (upper_value - lower_value) * (index - lower_index);
    return result;
}

double quantile(const std::unique_ptr<std::vector<double>>& data_ptr, double percentile) {
    if (data_ptr->empty() || percentile < 0 || percentile > 1) {
        return 0.0;
    }

    // Create a copy of the input data to avoid modifying the original vector
    std::vector<double> data_copy = *data_ptr;

    if (percentile == 0) {
        return *std::min_element(data_copy.begin(), data_copy.end());
    }
    if (percentile == 1) {
        return *std::max_element(data_copy.begin(), data_copy.end());
    }

    double index = (data_copy.size() - 1) * percentile;
    int lower_index = static_cast<int>(std::floor(index));
    int upper_index = static_cast<int>(std::ceil(index));

    std::nth_element(data_copy.begin(), data_copy.begin() + lower_index, data_copy.end());
    double lower_value = data_copy.at(lower_index);

    if (lower_index == upper_index) {
        return lower_value;
    }

    std::nth_element(data_copy.begin(), data_copy.begin() + upper_index, data_copy.end());
    double upper_value = data_copy.at(upper_index);

    double result = lower_value + (upper_value - lower_value) * (index - lower_index);
    return result;
}


double round_to_precision(double value, int precision) {
    double factor = std::pow(10.0, precision);
    return std::round(value * factor) / factor;
}

double quantile_without_nan_and_duplicates(std::unique_ptr<std::vector<double>>& data_ptr, double percentile){
    std::unordered_set<double> unique_data;
    for(int i = 0; i < data_ptr->size(); i++){
        if(!std::isnan(data_ptr->at(i))){
            unique_data.insert(round_to_precision(data_ptr->at(i),8));
        }
    }
    std::vector<double> data_without_nan_and_duplicates(unique_data.begin(), unique_data.end());
    auto filtered_data_ptr = std::make_unique<std::vector<double>>( std::move(data_without_nan_and_duplicates) );
    return quantile(filtered_data_ptr ,percentile);
}

namespace{
    enum class pairwise_operator{
        ADD,
        SUB,
        MUL,
        DIV
    };
    template<typename T>
    concept Avx2PairwiseOpSupport = std::is_same_v<T,double> || std::is_same_v<T,int64_t> || std::is_same_v<T,uint64_t> || std::is_same_v<T,uint32_t>;

    template <typename T, pairwise_operator op>
    requires Avx2PairwiseOpSupport<T>
    std::unique_ptr<std::vector<T>> pairwise_op(const std::unique_ptr<std::vector<T>>& x_ptr, const std::unique_ptr<std::vector<T>>& y_ptr){
        if (x_ptr->size() != y_ptr->size()) {
            throw std::runtime_error("Vectors must have the same size.");
        }
        size_t vec_size = x_ptr->size();
        size_t simd_width = 256 / (sizeof(T)*8);
        size_t aligned_size = vec_size - vec_size % simd_width;
        auto result = std::make_unique<std::vector<T>>(vec_size);
        auto x_data = x_ptr->data();
        auto y_data = y_ptr->data();
        auto result_data = result->data();
        for (size_t i = 0; i < aligned_size; i += simd_width) {
            if constexpr (std::is_same_v<T,double>){
                __m256d x_vec = _mm256_loadu_pd(x_data + i);
                __m256d y_vec = _mm256_loadu_pd(y_data + i);
                __m256d result_vec = _mm256_setzero_pd();
                if constexpr (op == pairwise_operator::ADD){
                    result_vec = _mm256_add_pd(x_vec, y_vec);
                }else if constexpr (op == pairwise_operator::SUB){
                    result_vec = _mm256_sub_pd(x_vec, y_vec);
                }else if constexpr (op == pairwise_operator::MUL){
                    result_vec = _mm256_mul_pd(x_vec, y_vec);
                }else if constexpr (op == pairwise_operator::DIV){
                    result_vec = _mm256_div_pd(x_vec, y_vec);
                }else{
                    throw std::runtime_error("pairwise op not support for double");
                }
                _mm256_storeu_pd(result_data + i, result_vec);
            }else if constexpr (std::is_same_v<T,int64_t> || std::is_same_v<T,uint64_t>){
                __m256i x_vec = _mm256_loadu_si256(x_data + i);
                __m256i y_vec = _mm256_loadu_si256(y_data + i);
                __m256i result_vec = _mm256_setzero_si256(); 
                if constexpr (op == pairwise_operator::ADD){
                    result_vec = _mm256_add_epi64(x_vec, y_vec);
                }else if constexpr (op == pairwise_operator::SUB){
                    result_vec = _mm256_sub_epi64(x_vec, y_vec);
                }else {
                    throw std::runtime_error("pairwise op not support for int64");
                }
                _mm256_storeu_pd(result_data + i, result_vec);
            }else if constexpr (std::is_same_v<T,uint32_t> || std::is_same_v<T,int32_t>){
                __m256i x_vec = _mm256_loadu_si256(x_data + i);
                __m256i y_vec = _mm256_loadu_si256(y_data + i);
                __m256i result_vec = _mm256_setzero_si256();         
                if constexpr (op == pairwise_operator::ADD){
                    result_vec = _mm256_add_epi32(x_vec, y_vec);
                }else if constexpr (op == pairwise_operator::SUB){
                    result_vec = _mm256_sub_epi32(x_vec, y_vec);
                }else if constexpr (op == pairwise_operator::MUL){
                    result_vec = _mm256_mullo_epi32(x_vec, y_vec);
                }else {
                    throw std::runtime_error("pairwise op not support for int64");
                }
                _mm256_storeu_pd(result_data + i, result_vec);
            }else{
                throw std::runtime_error("Unknown type, check template requires");
            }
        }
        for (size_t i = aligned_size; i < vec_size; ++i) {
            if constexpr (op == pairwise_operator::ADD){
                result_data[i] = x_data[i] + y_data[i];
            }else if constexpr (op == pairwise_operator::SUB){
                result_data[i] = x_data[i] - y_data[i];
            }else if constexpr (op == pairwise_operator::MUL){
                result_data[i] = x_data[i] * y_data[i];
            }else if constexpr (op == pairwise_operator::DIV){
                result_data[i] = x_data[i] / y_data[i];
            }
        }
        return std::move(result);
    }
}

namespace nan_ignore_impl{
    double mean(std::unique_ptr<std::vector<double>>& data_ptr) {
        if(data_ptr->size() == 1){
            return data_ptr->front();
        }
        uint64_t no_nan_count = 0;
        double sum_of = 0;
        for(size_t i = 0; i < data_ptr->size(); i++){
            if(!std::isnan(data_ptr->at(i))){
                sum_of += data_ptr->at(i);
                no_nan_count++;
            }
        }
        return sum_of / no_nan_count;
    }

    double max(std::unique_ptr<std::vector<double>>& data_ptr){
        if(data_ptr->size() == 1){
            return data_ptr->front();
        }
        double max_value = std::numeric_limits<double>::min();
        for(size_t i = 0; i < data_ptr->size(); i++){
            if(!std::isnan(data_ptr->at(i))){
                if(data_ptr->at(i) > max_value){
                    max_value = data_ptr->at(i);
                }
            }
        }
        return max_value;
    }

    double min(std::unique_ptr<std::vector<double>>& data_ptr){
        if(data_ptr->size() == 1){
            return data_ptr->front();
        }
        double min_value = std::numeric_limits<double>::max();
        for(size_t i = 0; i < data_ptr->size(); i++){
            if(!std::isnan(data_ptr->at(i))){
                if(data_ptr->at(i) < min_value){
                    min_value = data_ptr->at(i);
                }
            }
        }
        return min_value;
    }
}

namespace simd_impl{
    std::unique_ptr<std::vector<double>> multiply_scalar(const std::unique_ptr<std::vector<double>>& vec_ptr, double x) {
        if (!vec_ptr->size()) {
            throw std::invalid_argument("Input vector must not be empty.");
        }
        auto res_ptr = std::make_unique<std::vector<double>>(vec_ptr->size());

        constexpr size_t simd_size = xsimd::simd_type<double>::size;
        size_t vec_size = vec_ptr->size();

        for (size_t i = 0; i < vec_size - simd_size; i += simd_size) {
            auto batch = xsimd::load_unaligned(&vec_ptr->at(i));
            batch *= x;
            batch.store_unaligned(&res_ptr->at(i));
        }

        for (size_t i = vec_size - vec_size % simd_size; i < vec_size; ++i) {
            res_ptr->at(i) =  vec_ptr->at(i) * x;
        }
        return res_ptr;
    }

    //no simd, but add here
    std::unique_ptr<std::vector<double>> pairwise_log(const std::unique_ptr<std::vector<double>>& vec_ptr) {
        if (!vec_ptr->size()) {
            throw std::invalid_argument("Input vector must not be empty.");
        }
        auto res_ptr = std::make_unique<std::vector<double>>(vec_ptr->size());
        for(int i = 0; i < res_ptr->size(); i++){
            res_ptr->at(i) = std::log(vec_ptr->at(i));
        }
        return res_ptr;
    }




    template<typename T>
    std::unique_ptr<std::vector<T>> pairwise_add(const std::unique_ptr<std::vector<T>>& x_ptr, const std::unique_ptr<std::vector<T>>& y_ptr){
        return std::move(pairwise_op<T,pairwise_operator::ADD>(x_ptr,y_ptr));
    }

    template<typename T>
    std::unique_ptr<std::vector<T>> pairwise_sub(const std::unique_ptr<std::vector<T>>& x_ptr, const std::unique_ptr<std::vector<T>>& y_ptr){
        return std::move(pairwise_op<T,pairwise_operator::SUB>(x_ptr,y_ptr));
    }

    template<typename T>
    std::unique_ptr<std::vector<T>> pairwise_mul(const std::unique_ptr<std::vector<T>>& x_ptr, const std::unique_ptr<std::vector<T>>& y_ptr){
        return std::move(pairwise_op<T,pairwise_operator::MUL>(x_ptr,y_ptr));
    }    
    
    template<typename T>
    std::unique_ptr<std::vector<T>> pairwise_div(const std::unique_ptr<std::vector<T>>& x_ptr, const std::unique_ptr<std::vector<T>>& y_ptr){
        return std::move(pairwise_op<T,pairwise_operator::DIV>(x_ptr,y_ptr));
    }
    template<typename T>
    std::tuple<T, T, T, T> min_max_start_end(const std::unique_ptr<std::vector<T>>& input_ptr) {
        if (!input_ptr->size()) {
            throw std::invalid_argument("Input vector must not be empty.");
        }
        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;
        simd_type min_val = input_ptr->at(0), max_val = input_ptr->at(0);
        std::size_t i = 0;
        for (; i + simd_size <= input_ptr->size(); i += simd_size) {
            simd_type chunk = xsimd::load_unaligned(&input_ptr->at(i));

            min_val = xsimd::min(min_val, chunk);
            max_val = xsimd::max(max_val, chunk);
        }
        T min_value = xsimd::reduce_min(min_val);
        T max_value = xsimd::reduce_max(max_val);

        for (; i < input_ptr->size(); ++i) {
            min_value = std::min(min_value, input_ptr->at(i));
            max_value = std::max(max_value, input_ptr->at(i));
        }
        return std::make_tuple(min_value, max_value, input_ptr->front(), input_ptr->back());
    }

    template<typename T>
    T min(const std::unique_ptr<std::vector<T>>& input_ptr) {
        if (!input_ptr->size()) {
            throw std::invalid_argument("Input vector must not be empty.");
        }
        if(input_ptr->size() == 1){
            return input_ptr->front();
        }
        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;
        simd_type min_val = input_ptr->at(0);
        std::size_t i = 0;
        for (; i + simd_size <= input_ptr->size(); i += simd_size) {
            simd_type chunk = xsimd::load_unaligned(&input_ptr->at(i));
            min_val = xsimd::min(min_val, chunk);
        }
        T min_value = xsimd::reduce_min(min_val);

        for (; i < input_ptr->size(); ++i) {
            min_value = std::min(min_value, input_ptr->at(i));
        }
        return min_value;
    }

    template<typename T>
    T max(const std::unique_ptr<std::vector<T>>& input_ptr) {
        if (!input_ptr->size()) {
            throw std::invalid_argument("Input vector must not be empty.");
        }
        if(input_ptr->size() == 1){
            return input_ptr->front();
        }
        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;
        simd_type max_val = input_ptr->at(0);
        std::size_t i = 0;
        for (; i + simd_size <= input_ptr->size(); i += simd_size) {
            simd_type chunk = xsimd::load_unaligned(&input_ptr->at(i));
            max_val = xsimd::max(max_val, chunk);
        }
        T max_value = xsimd::reduce_max(max_val);

        for (; i < input_ptr->size(); ++i) {
            max_value = std::max(max_value, input_ptr->at(i));
        }
        return max_value;
    }

    template<typename T>
    T sum(const std::unique_ptr<std::vector<T>>& input_ptr) {
        if(input_ptr->size() == 1){
            return input_ptr->front();
        }
        if (!input_ptr->size()) {
            return static_cast<T>(0);
        }
        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;
        simd_type sum_val = 0;
        std::size_t i = 0;
        for (; i + simd_size <= input_ptr->size(); i += simd_size) {
            simd_type chunk = xsimd::load_unaligned(&input_ptr->at(i));
            sum_val = sum_val + chunk;
        }
        T sum_value = xsimd::reduce_add(sum_val);

        for (; i < input_ptr->size(); ++i) {
            sum_value += input_ptr->at(i);
        }
        return sum_value;
    }

    template<typename T>
    requires std::is_convertible_v<T,double>
    double mean(const std::unique_ptr<std::vector<T>>& data_ptr) {
        double res = static_cast<double>(sum<T>(data_ptr));
        return res / data_ptr-> size();
    }

    double std(const std::unique_ptr<std::vector<double>>& data_ptr, double mean) {
        double result = 0.0;
        size_t i = 0;
        const size_t simd_width = 4;
        const size_t vec_size = data_ptr->size();

        __m256d sum_vec = _mm256_setzero_pd();
        __m256d mean_vec = _mm256_set1_pd(mean);

        for (; i + simd_width <= vec_size; i += simd_width) {
            __m256d val_vec = _mm256_loadu_pd(&data_ptr->at(i));
            __m256d diff_vec = _mm256_sub_pd(val_vec, mean_vec);
            __m256d square_diff_vec = _mm256_mul_pd(diff_vec, diff_vec);
            sum_vec = _mm256_add_pd(sum_vec, square_diff_vec);
        }
        
        double sum_array[simd_width];
        _mm256_storeu_pd(sum_array, sum_vec);
        for (size_t j = 0; j < simd_width; ++j) {
            result += sum_array[j];
        }

        for (; i < vec_size; ++i) {
            double diff = data_ptr->at(i) - mean;
            result += diff * diff;
        }

        double variance = result / (data_ptr->size() - 1);
        return std::sqrt(variance);
    }

    double corr(std::unique_ptr<std::vector<double>>& x_ptr , std::unique_ptr<std::vector<double>>& y_ptr) {
        double mean_x = mean(x_ptr);
        double mean_y = mean(y_ptr);
        double covariance = 0.0;

        size_t i = 0;
        const size_t simd_width = 4;
        const size_t vec_size = x_ptr->size();

        __m256d variance_x_vec = _mm256_setzero_pd();
        __m256d variance_y_vec = _mm256_setzero_pd();
        __m256d cov_xy_vec = _mm256_setzero_pd();

        __m256d mean_vec_x = _mm256_set1_pd(mean_x);
        __m256d mean_vec_y = _mm256_set1_pd(mean_y);

        for (; i + simd_width <= vec_size; i += simd_width) {
            __m256d chunk_x_vec = _mm256_loadu_pd(&x_ptr->at(i));
            __m256d chunk_y_vec = _mm256_loadu_pd(&y_ptr->at(i));

            __m256d chunk_xdiff_vec = _mm256_sub_pd(chunk_x_vec, mean_vec_x);
            __m256d chunk_ydiff_vec = _mm256_sub_pd(chunk_y_vec, mean_vec_y);
            
            __m256d square_xdiff_vec = _mm256_mul_pd(chunk_xdiff_vec, chunk_xdiff_vec);
            __m256d square_ydiff_vec = _mm256_mul_pd(chunk_ydiff_vec, chunk_ydiff_vec);

            __m256d mul_xydiff_vec = _mm256_mul_pd(chunk_xdiff_vec, chunk_ydiff_vec);

            variance_x_vec = _mm256_add_pd(variance_x_vec, square_xdiff_vec);
            variance_y_vec = _mm256_add_pd(variance_y_vec, square_ydiff_vec);
            cov_xy_vec = _mm256_add_pd(cov_xy_vec, mul_xydiff_vec);
        }

        double variance_x_array[simd_width];
        double variance_y_array[simd_width];
        double cov_xy_array[simd_width];
        _mm256_storeu_pd(variance_x_array, variance_x_vec);
        _mm256_storeu_pd(variance_y_array, variance_y_vec);
        _mm256_storeu_pd(cov_xy_array, cov_xy_vec);

        double var_x = 0.0;
        double var_y = 0.0;
        double cov_xy = 0.0;
        for (size_t j = 0; j < simd_width; ++j) {
            var_x += variance_x_array[j];
            var_y += variance_y_array[j];
            cov_xy += cov_xy_array[j];
        }

        for (; i < vec_size; ++i) {
            double diff_x = x_ptr->at(i) - mean_x;
            double diff_y = y_ptr->at(i) - mean_y;
            var_x += diff_x * diff_x;
            var_y += diff_y * diff_y;

            cov_xy += diff_x * diff_y; 
        }
        return cov_xy / std::sqrt(var_x * var_y);
    }
}

namespace xsimd_aligned{
    template<typename T>
    T sum(const std::unique_ptr<std::vector<T,xsimd::aligned_allocator<T>>>& input_ptr) {
        if (!input_ptr->size()) {
            return static_cast<T>(0);
        }
        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;
        simd_type sum_val = 0;
        std::size_t i = 0;
        for (; i + simd_size <= input_ptr->size(); i += simd_size) {
            simd_type chunk = xsimd::load_aligned(&input_ptr->at(i));
            sum_val = sum_val + chunk;
        }
        T sum_value = xsimd::reduce_add(sum_val);

        for (; i < input_ptr->size(); ++i) {
            sum_value += input_ptr->at(i);
        }
        return sum_value;
    }

    template<typename T>
    std::tuple<T, T, T, T> min_max_start_end(const std::unique_ptr<std::vector<T,xsimd::aligned_allocator<T>>>& input_ptr) {
        if (!input_ptr->size()) {
            throw std::invalid_argument("Input vector must not be empty.");
        }
        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;
        simd_type min_val = input_ptr->at(0), max_val = input_ptr->at(0);
        std::size_t i = 0;
        for (; i + simd_size <= input_ptr->size(); i += simd_size) {
            simd_type chunk = xsimd::load_aligned(&input_ptr->at(i));

            min_val = xsimd::min(min_val, chunk);
            max_val = xsimd::max(max_val, chunk);
        }
        T min_value = xsimd::reduce_min(min_val);
        T max_value = xsimd::reduce_max(max_val);

        for (; i < input_ptr->size(); ++i) {
            min_value = std::min(min_value, input_ptr->at(i));
            max_value = std::max(max_value, input_ptr->at(i));
        }
        return std::make_tuple(min_value, max_value, input_ptr->front(), input_ptr->back());
    }

    template<typename T>
    std::unique_ptr<std::vector<T, xsimd::aligned_allocator<T>>> pairwise_mul(const std::unique_ptr<std::vector<T, xsimd::aligned_allocator<T>>>& a_ptr, const std::unique_ptr<std::vector<T, xsimd::aligned_allocator<T>>>& b_ptr) {
        if (a_ptr->size() != b_ptr->size()) {
            throw std::invalid_argument("Input vectors must have the same size.");
        }

        using simd_type = xsimd::simd_type<T>;
        std::size_t simd_size = simd_type::size;

        std::unique_ptr<std::vector<T, xsimd::aligned_allocator<T>>> res_ptr = std::make_unique<std::vector<T, xsimd::aligned_allocator<T>>>(a_ptr->size());
        std::size_t i = 0;
        for (; i + simd_size <= a_ptr->size(); i += simd_size) {
            simd_type chunk_a = xsimd::load_aligned(&a_ptr->at(i));
            simd_type chunk_b = xsimd::load_aligned(&b_ptr->at(i));
            simd_type product_chunk = chunk_a * chunk_b;
            xsimd::store_aligned(&res_ptr->at(i), product_chunk);
        }

        for (; i < a_ptr->size(); ++i) {
            res_ptr->at(i) = a_ptr->at(i)* b_ptr->at(i);
        }
        return res_ptr;
    }
}

namespace group{
    // enum class Function{
    //     COUNT,
    //     SUM,
    //     MAX,
    //     MIN,
    //     FIRST,
    //     LAST,
    //     QUANTILE,
    //     STD
    // };
    template<typename T>
    class Result{
    public:
        std::vector<T> groups_values;
        std::vector<uint32_t> group_to;
        absl::flat_hash_map<T,uint32_t> value_to_index;
        std::vector<uint32_t> index_count;
        std::vector<uint32_t> pre_index_to_ordered;
        std::vector<uint32_t> ordered_index_to_pre;
        Result(size_t s){ group_to.resize(s);}
        std::unique_ptr<std::vector<T>> get_group_values(){
            auto res_ptr = std::make_unique<std::vector<T>>( groups_values.size() );
            for(int i = 0; i < groups_values.size(); i++){
                res_ptr->at(ordered_index_to_pre[i]) = groups_values[i];
            }
            return res_ptr;
        }
        // template<typename ApplyType, Function F_type>
        // auto transform_on(std::unique_ptr<std::vector<ApplyType>>& data_ptr);
    };

    template<typename T>
    Result<T> by(std::unique_ptr<std::vector<T>>& col_ptr, std::string sort_method = "descending"){
        Result<T> res(col_ptr->size());
        uint32_t group_index = -1;
        for(size_t i = 0; i < col_ptr->size(); i++){
            auto iter = res.value_to_index.find( col_ptr->at(i) );
            //check value if existed
            if( iter == res.value_to_index.end() ){
                //value is not exisited, create a new group
                group_index++;  
                res.value_to_index.insert({col_ptr->at(i),group_index});
                res.group_to[i] = group_index;
                
            } else {
                res.group_to[i] = iter->second;
            }   
        }
        res.index_count.resize(group_index + 1 , 0);
        for(auto& to : res.group_to){
            res.index_count[to]++;
        }

        //each group's corresponding value
        res.groups_values.resize(group_index + 1);
        for(auto iter = res.value_to_index.begin(); iter != res.value_to_index.end(); iter++){
            res.groups_values[iter->second] = iter->first;
        }

        //make mapping with sort and unsort
        res.pre_index_to_ordered.resize(group_index + 1);
        std::iota(res.pre_index_to_ordered.begin(), res.pre_index_to_ordered.end(), 0);

        if(sort_method == "descending"){
            std::sort(res.pre_index_to_ordered.begin(), res.pre_index_to_ordered.end(),
            [&res](size_t i1, size_t i2) { return res.groups_values[i1] < res.groups_values[i2]; });
        }else if(sort_method == "ascending"){
            std::sort(res.pre_index_to_ordered.begin(), res.pre_index_to_ordered.end(),
            [&res](size_t i1, size_t i2) { return res.groups_values[i1] > res.groups_values[i2]; });
        }else{
            //just keep previous order
        }

        res.ordered_index_to_pre.resize(group_index + 1);
        for(int i = 0; i < res.pre_index_to_ordered.size(); i++){
            res.ordered_index_to_pre[res.pre_index_to_ordered[i]] = i; 
        }
        
        return res;
    }

    //for lvalue_ref and rvalue_ref same usage, no need of std::forward
    template<typename T, typename GroupbyResType>
    std::vector<std::unique_ptr<std::vector<T>>> partition(GroupbyResType&& res, std::unique_ptr<std::vector<T>>& data_ptr){
        std::vector<std::unique_ptr<std::vector<T>>> partition_res(res.index_count.size());
        //pre_allocated memory
        std::vector<uint32_t> each_filled(partition_res.size(), 0);
        for(uint32_t i = 0; i < partition_res.size(); i++){
            partition_res[i] = std::make_unique<std::vector<T>>(res.index_count[i]);
        }
        //partitioncount
        for(uint32_t i = 0; i < res.group_to.size(); i++){
            uint32_t group_index = res.group_to[i];
            partition_res[group_index]->at(each_filled[group_index]) = data_ptr->at(i);
            each_filled[group_index]++;
        }
        return partition_res;
    };

    namespace transform{
        template<typename T>
        std::unique_ptr<std::vector<uint64_t>> count(Result<T>& res){
            auto res_ptr = std::make_unique<std::vector<uint64_t>>(res.index_count.size());
            for(int i = 0; i < res_ptr->size(); i++){
                res_ptr->at(i) = res.index_count[static_cast<uint32_t>(i)];
            }
            auto transformed_ptr = std::make_unique<std::vector<uint64_t>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            //res_ptr->at(ordered_index_to_pre[i]) = groups_values[i];
            return std::move(transformed_ptr);
        }

        template<typename T>
        std::unique_ptr<std::vector<double>> count_as_double(Result<T>& res){
            auto res_ptr = std::make_unique<std::vector<double>>(res.index_count.size());
            for(int i = 0; i < res_ptr->size(); i++){
                res_ptr->at(i) = static_cast<double>(res.index_count[static_cast<uint32_t>(i)]);
            }
            auto transformed_ptr = std::make_unique<std::vector<double>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> sum(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(i) = simd_impl::sum(partitioned_data[i]);
            }
            auto transformed_ptr = std::make_unique<std::vector<T2>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> max(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(i) = simd_impl::max(partitioned_data[i]);
            }
            auto transformed_ptr = std::make_unique<std::vector<T2>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> min(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(i) = simd_impl::min(partitioned_data[i]);
            }
            auto transformed_ptr = std::make_unique<std::vector<T2>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }
        
        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> first(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(i) = partitioned_data[i]->front();
            }
            auto transformed_ptr = std::make_unique<std::vector<T2>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> last(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(i) = partitioned_data[i]->back();
            }
            auto transformed_ptr = std::make_unique<std::vector<T2>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }

        template<typename T1, typename T2>
        requires std::is_convertible_v<T2,double>
        std::unique_ptr<std::vector<double>> mean(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<double>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(i) = simd_impl::mean<T2>(partitioned_data[i]);
            }
            auto transformed_ptr = std::make_unique<std::vector<double>>(res.group_to.size());
            for(int i = 0; i < transformed_ptr->size(); i++){
                transformed_ptr->at(res.ordered_index_to_pre[i]) = res_ptr->at(i);
            }
            return std::move(transformed_ptr);
        }
    };

    namespace aggregate{
        template<typename T>
        std::unique_ptr<std::vector<uint64_t>> count(Result<T>& res){
            auto res_ptr = std::make_unique<std::vector<uint64_t>>(res.index_count.size());
            for(int i = 0; i < res_ptr->size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = static_cast<uint64_t>(res.index_count[i]);
            }
            return std::move(res_ptr);
        }

        template<typename T>
        std::unique_ptr<std::vector<double>> count_as_double(Result<T>& res){
            auto res_ptr = std::make_unique<std::vector<double>>(res.index_count.size());
            for(int i = 0; i < res_ptr->size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = static_cast<double>(res.index_count[i]);
            }
            return std::move(res_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> sum(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = simd_impl::sum(partitioned_data[i]);
            }
            return std::move(res_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> max(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = simd_impl::max(partitioned_data[i]);
            }
            return std::move(res_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> min(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = simd_impl::min(partitioned_data[i]);
            }
            return std::move(res_ptr);
        }
        
        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> first(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = partitioned_data[i]->front();
            }
            return std::move(res_ptr);
        }

        template<typename T1, typename T2>
        std::unique_ptr<std::vector<T2>> last(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<T2>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = partitioned_data[i]->back();
            }
            return std::move(res_ptr);
        }

        template<typename T1, typename T2>
        requires std::is_convertible_v<T2,double>
        std::unique_ptr<std::vector<double>> mean(Result<T1>& res, std::vector<std::unique_ptr<std::vector<T2>>>& partitioned_data){
            auto res_ptr = std::make_unique<std::vector<double>>(partitioned_data.size());
            for(int i = 0; i < partitioned_data.size(); i++){
                res_ptr->at(res.ordered_index_to_pre[i]) = simd_impl::mean<T2>(partitioned_data[i]);
            }
            return std::move(res_ptr);
        }

    };

    // template<typename T>
    // template<typename ApplyType, Function F_type>
    // auto Result<T>::transform_on(std::unique_ptr<std::vector<ApplyType>>& data_ptr){
    //     if constexpr (F_type == Function::COUNT){
    //         return std::move(group::transform::count(*this));
    //     }else{
    //         auto partition_res = group::partition(*this,data_ptr);
    //         if constexpr (F_type == Function::FIRST){
    //             return std::move(group::transform::first(*this,partition_res));
    //         }else if constexpr (F_type == Function::LAST){
    //             return std::move(group::transform::last(*this,partition_res));
    //         }else if constexpr (F_type == Function::MAX){
    //             return std::move(group::transform::max(*this,partition_res));
    //         }else if constexpr (F_type == Function::MIN){
    //             return std::move(group::transform::min(*this,partition_res));
    //         }else if constexpr (F_type == Function::SUM){
    //             return std::move(group::transform::sum(*this,partition_res));
    //         }else if constexpr (F_type == Function::QUANTILE){

    //         }
    //     } 
    //     return nullptr;
    // }
}


template<typename T>
std::unique_ptr<std::vector<uint32_t>> satisfy(std::unique_ptr<std::vector<T>>& data_ptr, std::function<bool(const T&)> functor){
    std::vector<uint32_t> if_satisfy(data_ptr->size(),0);
    for(int i = 0; i < if_satisfy.size(); i++){
        if(functor(data_ptr->at(i))){
            if_satisfy[i] = 1;
        }
    }
    return std::make_unique<std::vector<uint32_t>>(std::move(if_satisfy));
}

template<typename T>
std::unique_ptr<std::vector<uint32_t>> satisfy(std::unique_ptr<std::vector<T>>& data_ptr1, std::unique_ptr<std::vector<T>>& data_ptr2, std::function<bool(const T& v1, const T& v2)> functor){
    std::vector<uint32_t> if_satisfy(data_ptr1->size(),0);
    for(int i = 0; i < if_satisfy.size(); i++){
        if(functor(data_ptr1->at(i), data_ptr2->at(i))){
            if_satisfy[i] = 1;
        }
    }
    return std::make_unique<std::vector<uint32_t>>(std::move(if_satisfy));
}

template<typename T>
std::unique_ptr<std::vector<T>> selected_by(std::unique_ptr<std::vector<T>>& data_ptr, std::unique_ptr<std::vector<uint32_t>>& place_holder_ptr, uint32_t selected_count){
    auto res_ptr = std::make_unique<std::vector<T>>(selected_count);
    uint32_t index = 0;
    for(auto i = 0; i < place_holder_ptr->size(); i++){
        if( place_holder_ptr->at(i) == 1){
            res_ptr->at(index) = data_ptr->at(i);
            index++;
        }
    }
    return std::move(res_ptr);
}

template<typename T>
T sumVector(const std::vector<T>& nums, int start, int end) {
    if (start == end) {
        int n = nums[0]->size();
        auto res_ptr = std::make_unique<std::vector<double>>(n);
        std::memcpy(res_ptr->data(), nums[start]->data(),n*sizeof(double));
        return res_ptr;
    } else {
        int mid = (start + end) / 2;
        T leftSum = sumVector(nums, start, mid);  
        T rightSum = sumVector(nums, mid + 1, end); 
        return simd_impl::pairwise_add(leftSum , rightSum);  
    }
}

template<typename T>
T sumVector(const std::vector<T>& nums) {
    return sumVector(nums, 0, nums.size() - 1);
}



#endif