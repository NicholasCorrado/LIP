#ifndef CSV_TO_ARROW_CPP_SELECT_H
#define CSV_TO_ARROW_CPP_SELECT_H

#endif //CSV_TO_ARROW_CPP_SELECT_H


#include "BloomFilter.h"



BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, Operator op);
