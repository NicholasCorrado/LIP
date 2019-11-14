
#include "JoinParam.h"

void SelectParam::SetType(SelectType type){
	sType = type;
}

SelectType SelectParam::GetType(){
	return sType;
}

void SelectParam::SetDimTable(std::shared_ptr<arrow::Table> _dimTable){
	dimTable = _dimTable;
}


std::shared_ptr<arrow::Table> SelectParam::GetDimTable(){
	return dimTable;
}

void SelectParam::SetSelectField(std::string _select_field){
	select_field = _select_field;
}

std::string SelectParam::GetSelectField(){
	return select_field;
}



SelectParamInt::SelectParamInt(std::shared_ptr<arrow::Table> _dimTable, std::string _select_field, long long _value, Operator _op){
	SetType(SelectType::TYPE_INT);
	SetDimTable(_dimTable);
	SetSelectField(_select_field);
	value = _value;
	op = _op;
}

long long SelectParamInt::GetValue(){
	return value;
}

Operator SelectParamInt::GetOperator(){
	return op;
}

void SelectParamInt::PrintParam(){
	std::cout << "type = " << GetType() << std::endl;
	std::cout << "select_field = " << GetSelectField() << std::endl;
	std::cout << "value = " << GetValue() << std::endl;
	std::cout << "operator = " << GetOperator() << std::endl;
}


SelectParamString::SelectParamString(std::shared_ptr<arrow::Table> _dimTable, std::string _select_field, std::string _value, Operator _op){
	SetType(SelectType::TYPE_STRING);
	SetDimTable(_dimTable);
	SetSelectField(_select_field);
	value = _value;
	op = _op;
}

std::string SelectParamString::GetValue(){
	return value;
}

Operator SelectParamString::GetOperator(){
	return op;
}

void SelectParamString::PrintParam(){
	std::cout << "type = " << GetType() << std::endl;
	std::cout << "select_field = " << GetSelectField() << std::endl;
	std::cout << "value = " << GetValue() << std::endl;
	std::cout << "operator = " << GetOperator() << std::endl;
}

SelectParamBetween::SelectParamBetween(std::shared_ptr<arrow::Table> _dimTable, std::string _select_field, long long _lo_value, long long _hi_value){
	SetType(SelectType::TYPE_BETWEEN);
	SetDimTable(_dimTable);
	SetSelectField(_select_field);
	lo_value = _lo_value;
	hi_value = _hi_value;
}

long long SelectParamBetween::GetLoValue(){
	return lo_value;
}

long long SelectParamBetween::GetHiValue(){
	return hi_value;
}


void SelectParamBetween::PrintParam(){
	std::cout << "type = " << GetType() << std::endl;
	std::cout << "select_field = " << GetSelectField() << std::endl;
	std::cout << "lo_value = " << GetLoValue() << std::endl;
	std::cout << "hi_value = " << GetHiValue() << std::endl;
}

JoinParam::JoinParam(std::string _fact_foreign_key, std::string _dim_primary_key, SelectParam* _sParam){
	fact_foreign_key = _fact_foreign_key;
	dim_primary_key = _dim_primary_key;
	sParam = _sParam;
}

std::string JoinParam::GetFactForeignKey(){
	return fact_foreign_key;
}

std::string JoinParam::GetDimPrimaryKey(){
	return dim_primary_key;
}

SelectParam* JoinParam::GetSelectParam(){
	return sParam;
}

void JoinParam::PrintParam(){
	std::cout << "fact key = " << GetFactForeignKey() << std::endl;
	std::cout << "dim  key = " << GetDimPrimaryKey() << std::endl;
	sParam -> PrintParam();

}

