#ifndef FEATURE_CALLBACK
#define FEATURE_CALLBACK
#include <memory>
#include "../common/info.hpp"

template <typename T>
class FeatureCallBack{
public:
    virtual std::vector<double> calculate(const PackedInfoSp<T>& tick_buffer_sp, const int index);
};

template <typename T>
using CallBackObjPtr = std::unique_ptr<FeatureCallBack<T>>;
#endif
