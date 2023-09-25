#ifndef FEATURE_CALLBACK
#define FEATURE_CALLBACK
#include <memory>
#include "../common/info.hpp"

template <typename T>
class FeatureCallBack{
public:
    virtual std::vector<double> calculate(const PackedInfoSp<T>& tick_buffer_sp, int index) = 0;
    virtual ~FeatureCallBack() = default;
};

template <typename T>
using CallBackObjPtr = std::unique_ptr<FeatureCallBack<T>>;
#endif
