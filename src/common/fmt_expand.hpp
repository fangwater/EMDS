#ifndef FMT_EXPAND_HPP
#define FMT_EXPAND_HPP

#include <array>
#include <string_view>
#include <fmt/format.h>
#include <filesystem>

namespace fmt {
    template <>
    struct formatter<std::array<char, 11>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const std::array<char, 11>& arr, FormatContext& ctx) {
        return format_to(ctx.out(), "{}", std::string_view(arr.data(), 11));
    }
};

template <>
struct formatter<std::filesystem::path> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const std::filesystem::path &path, FormatContext &ctx) {
        return format_to(ctx.out(), "{}", path.string());
    }
};

}
#endif
