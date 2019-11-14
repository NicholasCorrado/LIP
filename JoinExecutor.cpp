
#include "JoinExecutor.h"
#include "select.h"
#include "Join.h"





SelectExecutorCompare::SelectExecutorCompare(std::shared_ptr<arrow::Table> _dim_table, std::string _select_field, int _value, arrow::compute::CompareOperator _op){
	dim_table = _dim_table;
	select_field = _select_field;

	arrow::NumericScalar<arrow::Int64Type> myscalar(_value);
    value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);
	op = _op;
}

std::shared_ptr<arrow::Table> SelectExecutorCompare::select(){
	std::shared_ptr<arrow::Table> ret;
	ret = Select(dim_table, select_field, value, op);
	return ret;
}

// void SelectParamCompare::PrintParam(){
// 	std::cout << "type = " << GetType() << std::endl;
// 	std::cout << "select_field = " << GetSelectField() << std::endl;
// 	std::cout << "value = " << GetValue() << std::endl;
// 	std::cout << "operator = " << GetOperator() << std::endl;
// }




SelectExecutorBetween::SelectExecutorBetween(std::shared_ptr<arrow::Table> _dim_table, std::string _select_field, long long _hi_value, long long _lo_value){
	dim_table = _dim_table;
	select_field = _select_field;
	hi_value = _hi_value;
	lo_value = _lo_value;
}

std::shared_ptr<arrow::Table> SelectExecutorBetween::select(){
	std::shared_ptr<arrow::Table> ret;
	//ret = Select(dim_table, select_field, lo_value, hi_value);
	return ret;
}


// void SelectParamBetween::PrintParam(){
// 	std::cout << "type = " << GetType() << std::endl;
// 	std::cout << "select_field = " << GetSelectField() << std::endl;
// 	std::cout << "lo_value = " << GetLoValue() << std::endl;
// 	std::cout << "hi_value = " << GetHiValue() << std::endl;
// }


JoinExecutor::JoinExecutor(SelectExecutor* _s_exe, std::string _dim_primary_key){
	dim_primary_key = _dim_primary_key;
	select_exe = _s_exe;
}

std::shared_ptr<arrow::Table> JoinExecutor::join(std::shared_ptr<arrow::Table> fact_table, 
													std::string fact_foreign_key){

	std::shared_ptr<arrow::Table> dim_table_selected = select_exe -> select();
	std::shared_ptr<arrow::Table> ret;
	ret = HashJoin(fact_table, fact_foreign_key, dim_table_selected, dim_primary_key);
	return ret;
}

