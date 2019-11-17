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

	std::shared_ptr<arrow::RecordBatch> select(std::shared_ptr<arrow::RecordBatch> in_batch);

	virtual arrow::compute::Datum* GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch) = 0;
};

class SelectExecutorComposite : public SelectExecutor{
public:
	std::vector<SelectExecutor*> children;
	
	SelectExecutorComposite(std::vector<SelectExecutor*> _children);
	arrow::compute::Datum* GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch);
};




class SelectExecutorInt : public SelectExecutor{
public:
	std::string select_field;
	std::shared_ptr<arrow::Scalar> value;
	arrow::compute::CompareOperator op;


	SelectExecutorInt(std::string _select_field,
							long long _value, 
							arrow::compute::CompareOperator _op);

	arrow::compute::Datum* GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch);
};


class SelectExecutorStr : public SelectExecutor{
public:
	std::string select_field;
	std::string value;
	arrow::compute::CompareOperator op;

	SelectExecutorStr(std::string _select_field,
							std::string _value, 
							arrow::compute::CompareOperator _op);
	arrow::compute::Datum* GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch);
};


class SelectExecutorBetween : public SelectExecutor{
public:
	std::string select_field;
	std::shared_ptr<arrow::Scalar> lo_value;
	std::shared_ptr<arrow::Scalar> hi_value;

	SelectExecutorBetween(
							std::string _select_field, 
							long long _lo_value, 
							long long _hi_value);
	arrow::compute::Datum* GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch);
};

class SelectExecutorStrBetween : public SelectExecutor{
public:

    std::string select_field;
    std::string lo_value;
    std::string hi_value;

    SelectExecutorStrBetween(
                          std::string _select_field,
                          std::string _lo_value,
                          std::string _hi_value);
    arrow::compute::Datum* GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch);
};


class SelectExecutorTree {
public:
    std::shared_ptr<arrow::Table> dim_table;
    SelectExecutor* root;

    SelectExecutorTree(std::shared_ptr<arrow::Table> _dim_table, SelectExecutor* _root);
    std::shared_ptr<arrow::Table> Select();
    BloomFilter* ConstructBloomFilterNoFK(std::string dim_primary_key);
};



class JoinExecutor{
private:
	std::string dim_primary_key;
	std::string fact_foreign_key;
	SelectExecutorTree* select_tree_exe;
public:

    JoinExecutor(SelectExecutorTree* _s_tree_exe,
                 std::string _dim_primary_key,
                 std::string fact_foreign_key);

	std::shared_ptr<arrow::Table> join(std::shared_ptr<arrow::Table> fact_table);
	BloomFilter* ConstructBloomFilter();
};


#endif