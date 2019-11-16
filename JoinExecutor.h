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


	std::shared_ptr<arrow::Table> select();
	BloomFilter* ConstructFilterNoFK(std::string dim_primary_key);

	virtual arrow::compute::Datum* GetBitFilter() = 0;
};

class SelectExecutorComposite{
public:
	std::vector<SelectExecutor*> children;
	
	SelectExecutorComposite(std::vector<SelectExecutor*> _children);
	arrow::compute::Datum* GetBitFilter();
};

class SelectExecutorInt : public SelectExecutor{
public:
	std::string select_field;
	std::shared_ptr<arrow::Scalar> value;
	arrow::compute::CompareOperator op;


	SelectExecutorInt(std::shared_ptr<arrow::Table> _dim_table, 
							std::string _select_field, 
							long long _value, 
							arrow::compute::CompareOperator _op);
	arrow::compute::Datum* GetBitFilter();
};


class SelectExecutorStr : public SelectExecutor{
public:
	std::string select_field;
	std::string value;
	arrow::compute::CompareOperator op;

	SelectExecutorStr(std::shared_ptr<arrow::Table> _dim_table, 
							std::string _select_field, 
							std::string _value, 
							arrow::compute::CompareOperator _op);
	arrow::compute::Datum* GetBitFilter();
};


class SelectExecutorBetween : public SelectExecutor{
public:
	std::string select_field;
	std::shared_ptr<arrow::Scalar> lo_value;
	std::shared_ptr<arrow::Scalar> hi_value;

	SelectExecutorBetween(std::shared_ptr<arrow::Table> _dim_table, 
							std::string _select_field, 
							long long _lo_value, 
							long long _hi_value);
	arrow::compute::Datum* GetBitFilter();
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