#ifndef CSV_TO_ARROW_CPP_SELECT_H
#define CSV_TO_ARROW_CPP_SELECT_H
#endif //CSV_TO_ARROW_CPP_SELECT_H


#include "BloomFilter.h"
#include "select.h"

//BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, Operator op, std::string key_field, std::string foreign_key);

//BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, long long value, Operator op, std::string key_field, std::string foreign_key);


BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table, 
							std::string select_field, 
							std::shared_ptr<arrow::Scalar> value, 
							arrow::compute::CompareOperator op, 
							std::string key_field);

BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table, 
							std::string select_field, 
							std::shared_ptr<arrow::Scalar> lo_value, 
							std::shared_ptr<arrow::Scalar> hi_value, 
							std::string key_field);

bool BloomFilterCompare( BloomFilter *lhs,  BloomFilter *rhs);
bool BloomFilterCompareK( BloomFilter *lhs,  BloomFilter *rhs);
bool BloomFilterCompareNick( BloomFilter *lhs,  BloomFilter *rhs);

BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table,
                            std::string select_field,
                            std::string value,
                            arrow::compute::CompareOperator op,
                            std::string key_field);

BloomFilter* BuildFilter(std::shared_ptr<arrow::Table> table,
                         std::string select_field,
                         std::string lo,
                         std::string hi,
                         std::string key_field);

