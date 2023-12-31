#ifndef TIME_UTILS_HPP
#define TIME_UTILS_HPP
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <absl/time/time.h>
#include <chrono>
#include <fmt/chrono.h>

struct ShanghaiTimeZone {
    absl::TimeZone tz;
    ShanghaiTimeZone() {
        absl::LoadTimeZone("Asia/Shanghai", &tz);
    }
}sh_tz;

std::string get_current_time_as_string() {
    auto now = std::chrono::system_clock::now();
    now += std::chrono::hours(8);// 为东八区增加8小时的偏移
    return fmt::format("{:%Y-%m-%d %H:%M:%S}", now);
}

std::function<absl::Duration(std::string_view time_str)> convert_time_string_to_duration = [](std::string_view time_str){
    //09:15:00.040
    //hh:mm:ss.xxx
    int hour_tens = time_str[0] - '0';       // 小时部分十位数
    int hour_ones = time_str[1] - '0';       // 小时部分个位数
    int minute_tens = time_str[3] - '0';     // 分钟部分十位数
    int minute_ones = time_str[4] - '0';     // 分钟部分个位数
    int second_tens = time_str[6] - '0';     // 秒数部分十位数
    int second_ones = time_str[7] - '0';     // 秒数部分个位数
    int millisecond_hundreds = time_str[9] - '0';   // 毫秒部分百位数
    int millisecond_tens = time_str[10] - '0';       // 毫秒部分十位数
    int millisecond_ones = time_str[11] - '0';       // 毫秒部分个位数
    //std::cout <<  fmt::format("{}{}:{}{}:{}{}.{}{}{}",hour_tens,hour_ones,minute_tens,minute_ones,second_tens,second_ones,millisecond_hundreds,millisecond_tens,millisecond_ones) << std::endl;
    absl::Duration dur = absl::Milliseconds(millisecond_hundreds*100 + millisecond_tens*10 + millisecond_ones) + absl::Seconds(second_tens*10 + second_ones) + absl::Minutes(minute_tens*10 + minute_ones) + absl::Hours(hour_tens*10 + hour_ones);
    return dur;
};

std::function<bool(absl::Time&, absl::Time&)> is_same_civil_min = [](absl::Time& t1, absl::Time& t2)->bool {
    absl::CivilMinute minute1 = absl::ToCivilMinute(t1, sh_tz.tz);
    absl::CivilMinute minute2 = absl::ToCivilMinute(t2, sh_tz.tz);
    return minute1 == minute2;
};

std::function<bool(absl::Time&, absl::Time&)> is_next_civil_min = [](absl::Time& t1, absl::Time& t2)->bool {
    absl::CivilMinute minute1 = absl::ToCivilMinute(t1, sh_tz.tz);
    absl::CivilMinute minute2 = absl::ToCivilMinute(t2, sh_tz.tz);
    return (minute1+1) == minute2;
};

std::function<bool(absl::Time&, absl::Time&, int64_t)> is_next_k_ms = 
    [](absl::Time& t1, absl::Time& t2, int64_t k)->bool {
    absl::Duration passed = t2 - t1;
    return (passed >= absl::Milliseconds(k));
};

std::string duration_to_string(absl::Duration bias) {
    // 提取时、分、秒、毫秒
    int hours = absl::ToInt64Hours(bias);
    bias -= absl::Hours(hours);
    int minutes = absl::ToInt64Minutes(bias);
    bias -= absl::Minutes(minutes);
    int seconds = absl::ToInt64Seconds(bias);
    bias -= absl::Seconds(seconds);
    int milliseconds = absl::ToInt64Milliseconds(bias);
    char buffer[20];  // 为输出缓冲区分配空间
    snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds);

    return std::string(buffer);
}


#endif //TIME_UTILS_HPP
