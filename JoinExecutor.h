#ifndef JOIN_PARAM_H
#define JOIN_PARAM_H

#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>
#include <arrow/api.h>
#include "select.h"
#include "BuildFilter.h"
#include <map>




class SelectExecutor{
public:
	std::shared_ptr<arrow::Table> dim_table;
	std::string select_field;
	virtual std::shared_ptr<arrow::Table> select() = 0;
	virtual BloomFilter* ConstructFilterNoFK(std::string dim_primary_key) = 0;
};



class SelectExecutorInt : public SelectExecutor{
public:
	std::shared_ptr<arrow::Scalar> value;
	arrow::compute::CompareOperator op;

	SelectExecutorInt(std::shared_ptr<arrow::Table> _dim_table, 
							std::string _select_field, 
							long long _value, 
							arrow::compute::CompareOperator _op);
	std::shared_ptr<arrow::Table> select();
	BloomFilter* ConstructFilterNoFK(std::string dim_primary_key);
};


class SelectExecutorStr : public SelectExecutor{
public:
	std::string value;
	arrow::compute::CompareOperator op;

	SelectExecutorStr(std::shared_ptr<arrow::Table> _dim_table, 
							std::string _select_field, 
							std::string _value, 
							arrow::compute::CompareOperator _op);
	std::shared_ptr<arrow::Table> select();
	BloomFilter* ConstructFilterNoFK(std::string dim_primary_key);
};


class SelectExecutorBetween : public SelectExecutor{
public:
	std::shared_ptr<arrow::Scalar> lo_value;
	std::shared_ptr<arrow::Scalar> hi_value;

	SelectExecutorBetween(std::shared_ptr<arrow::Table> _dim_table, 
							std::string _select_field, 
							long long _lo_value, 
							long long _hi_value);
	std::shared_ptr<arrow::Table> select();
	BloomFilter* ConstructFilterNoFK(std::string dim_primary_key);
};

class SelectExecutorStrBetween : public SelectExecutor{
public:
    std::string lo_value;
    std::string hi_value;

    SelectExecutorStrBetween(std::shared_ptr<arrow::Table> _dim_table,
                          std::string _select_field,
                          std::string _lo_value,
                          std::string _hi_value);
    std::shared_ptr<arrow::Table> select();
    BloomFilter* ConstructFilterNoFK(std::string dim_primary_key);
};



class JoinExecutor{
private:
	std::string dim_primary_key;
	std::string fact_foreign_key;
	SelectExecutor* select_exe;
public:
	JoinExecutor(SelectExecutor* _s_exe, 
					std::string _dim_primary_key, 
					std::string fact_foreign_key);
	std::shared_ptr<arrow::Table> join(std::shared_ptr<arrow::Table> fact_table);
	BloomFilter* ConstructFilter();
};


#endif