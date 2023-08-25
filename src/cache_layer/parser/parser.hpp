#ifndef PARSER_HPP
#define PARSER_HPP
#include "../info_cache.hpp"
template<typename T>
class Parser{
public:
    std::shared_ptr<InfoCache<T>> InfoCache_ptr;
    explicit Parser( std::shared_ptr<InfoCache<T>> ptr ){
        InfoCache_ptr = ptr;
    }
    virtual void MessageProcess(std::string_view line) = 0;
};
#endif