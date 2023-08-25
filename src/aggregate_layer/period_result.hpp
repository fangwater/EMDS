#ifndef PERIODRESULT_HPP
#define PERIODRESULT_HPP
#include <absl/time/time.h>
#include <array>
#include <vector>
#include <memory>

struct PeriodResult{
    int period;
    std::array<char,11> security_id;
    std::vector<std::vector<double>> results;
};
#endif
