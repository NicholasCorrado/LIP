
#include "JoinExecutor.h"
#include "select.h"
#include "Join.h"
#include "BuildFilter.h"




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

std::shared_ptr<arrow::Table> SelectExecutorInt::select(){
	std::shared_ptr<arrow::Table> ret;
	ret = Select(dim_table, select_field, value, op);
	return ret;
}


BloomFilter* SelectExecutorInt::ConstructFilterNoFK(std::string dim_primary_key){
	BloomFilter* bf = BuildFilter(dim_table,
									select_field,
									value,
									op,
									dim_primary_key);
	return bf;
}

// void SelectParamCompare::PrintParam(){
// 	std::cout << "type = " << GetType() << std::endl;
// 	std::cout << "select_field = " << GetSelectField() << std::endl;
// 	std::cout << "value = " << GetValue() << std::endl;
// 	std::cout << "operator = " << GetOperator() << std::endl;
// }


SelectExecutorStr::SelectExecutorStr(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												std::string _value, 
												arrow::compute::CompareOperator _op){
	dim_table = _dim_table;
	select_field = _select_field;

	value = _value;
	op = _op;
}

std::shared_ptr<arrow::Table> SelectExecutorStr::select(){
	std::shared_ptr<arrow::Table> ret;
	ret = SelectString(dim_table, select_field, value, op);
	return ret;
}


BloomFilter* SelectExecutorStr::ConstructFilterNoFK(std::string dim_primary_key){
	BloomFilter* bf = BuildFilter(dim_table,
									select_field,
									value,
									op,
									dim_primary_key);
	return bf;
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


SelectExecutorStrBetween::SelectExecutorStrBetween(std::shared_ptr<arrow::Table> _dim_table,
                                             std::string _select_field,
                                             std::string _lo_value,
                                             std::string _hi_value) {
    dim_table = _dim_table;
    select_field = _select_field;

    lo_value = _lo_value;
    hi_value = _hi_value;
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

// void SelectParamBetween::PrintParam(){
// 	std::cout << "type = " << GetType() << std::endl;
// 	std::cout << "select_field = " << GetSelectField() << std::endl;
// 	std::cout << "lo_value = " << GetLoValue() << std::endl;
// 	std::cout << "hi_value = " << GetHiValue() << std::endl;
// }
BloomFilter* SelectExecutorBetween::ConstructFilterNoFK(std::string dim_primary_key){
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

