#ifndef JOIN_PARAM_H
#define JOIN_PARAM_H

#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>
#include <arrow/api.h>
#include "select.h"
#include <map>




class SelectExecutor{
public:
	std::shared_ptr<arrow::Table> dim_table;
	std::string select_field;
	virtual std::shared_ptr<arrow::Table> select() = 0;
};



class SelectExecutorCompare : public SelectExecutor{
public:
	std::shared_ptr<arrow::Scalar> value;
	arrow::compute::CompareOperator op;

	SelectExecutorCompare(std::shared_ptr<arrow::Table> _dim_table, std::string _select_field, int _value, arrow::compute::CompareOperator _op);
	std::shared_ptr<arrow::Table> select();
};


class SelectExecutorBetween : public SelectExecutor{
public:
	long long lo_value;
	long long hi_value;

	SelectExecutorBetween(std::shared_ptr<arrow::Table> _dim_table, std::string _select_field, long long _lo_value, long long _hi_value);
	std::shared_ptr<arrow::Table> select();
};




class JoinExecutor{
private:
	std::string dim_primary_key;
	
	SelectExecutor* select_exe;
public:
	JoinExecutor(SelectExecutor* _s_exe, std::string _dim_primary_key);
	std::shared_ptr<arrow::Table> join(std::shared_ptr<arrow::Table> fact_table, std::string fact_foreign_key);
};


#endif