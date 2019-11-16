
#include "JoinExecutor.h"
#include "select.h"
#include "Join.h"
#include "BuildFilter.h"


std::shared_ptr<arrow::Table> 
SelectExecutor::select(){

	arrow::compute::Datum* filter = GetBitFilter();

	std::shared_ptr<arrow::Table> ret;

	/*

		filter the result table here

	*/

	return ret;
}



BloomFilter* 
SelectExecutor::ConstructFilterNoFK(std::string dim_primary_key){

	arrow::compute::Datum* filter = GetBitFilter();

	int n = 0; /* the number of 1 in filter */

	BloomFilter* bf = new BloomFilter(n);

	/*

		add the bloom filter

	*/

	return bf;
}
	





SelectExecutorComposite::SelectExecutorComposite(std::vector<SelectExecutor*> _children){
	children = _children;
}


arrow::compute::Datum* 
SelectExecutorComposite::GetBitFilter(){
	int num_of_children = children.size();

	arrow::compute::Datum* ret; /*initialize it to all 0*/

	for(int child_i = 0; child_i < num_of_children; child_i++){
		arrow::compute::Datum* child_filter = children[child_i] -> GetBitFilter();

		/* ret = ret || child_filter */
	}
	return ret;
}





SelectExecutorInt::SelectExecutorInt(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												long long _value, 
												arrow::compute::CompareOperator _op){
	dim_table = _dim_table;
	select_field = _select_field;

	arrow::NumericScalar<arrow::Int64Type> myscalar(_value);
    value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);
	op = _op;
}



arrow::compute::Datum* 
SelectExecutorInt::GetBitFilter(){
	arrow::compute::Datum* ret;
	/* Get bit filter satisfying (integers op value) */
	return ret;
}



SelectExecutorStr::SelectExecutorStr(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												std::string _value, 
												arrow::compute::CompareOperator _op){
	dim_table = _dim_table;
	select_field = _select_field;

	value = _value;
	op = _op;
}


arrow::compute::Datum* 
SelectExecutorStr::GetBitFilter(){
	arrow::compute::Datum* ret;
	/* Get bit filter satisfying (string op value) */
	return ret;
	
}



SelectExecutorBetween::SelectExecutorBetween(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												long long _lo_value, 
												long long _hi_value){
	dim_table = _dim_table;
	select_field = _select_field;

	arrow::NumericScalar<arrow::Int64Type> lo_scalar(_lo_value);
    lo_value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(lo_scalar);
	
	arrow::NumericScalar<arrow::Int64Type> hi_scalar(_hi_value);
    hi_value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(hi_scalar);
}


arrow::compute::Datum* 
SelectExecutorBetween::GetBitFilter(){
	arrow::compute::Datum* ret;
	/* Get bit filter satisfying (lo <= integers <= hi) */
	return ret;
}

std::shared_ptr<arrow::Table> SelectExecutorStrBetween::select() {
    std::shared_ptr<arrow::Table> ret;
    ret = SelectStringBetween(dim_table, select_field, lo_value, hi_value);
    return ret;
}

std::shared_ptr<arrow::Table> SelectExecutorBetween::select() {
    std::shared_ptr<arrow::Table> ret;
    ret = SelectBetween(dim_table, select_field, lo_value, hi_value);
    return ret;
}

BloomFilter* SelectExecutorStrBetween::ConstructFilterNoFK(std::string dim_primary_key){
    BloomFilter* bf = BuildFilter(dim_table,
                                  select_field,
                                  lo_value,
                                  hi_value,
                                  dim_primary_key);
    return bf;
}





JoinExecutor::JoinExecutor(SelectExecutor* _s_exe, 
							std::string _dim_primary_key, 
							std::string _fact_foreign_key){
	fact_foreign_key = _fact_foreign_key;
	dim_primary_key = _dim_primary_key;
	select_exe = _s_exe;
}

std::shared_ptr<arrow::Table> JoinExecutor::join(std::shared_ptr<arrow::Table> fact_table){

	std::shared_ptr<arrow::Table> dim_table_selected = select_exe -> select();
	std::shared_ptr<arrow::Table> ret;
	ret = HashJoin(fact_table, fact_foreign_key, dim_table_selected, dim_primary_key);
	return ret;
}



BloomFilter* JoinExecutor::ConstructFilter(){
	BloomFilter* bf = select_exe -> ConstructFilterNoFK(dim_primary_key);
	bf -> SetForeignKey(fact_foreign_key);
	return bf;
}

