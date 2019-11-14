#ifndef JOIN_PARAM_H
#define JOIN_PARAM_H

#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>
#include <arrow/api.h>
#include "select.h"
#include <map>


enum SelectType {
	TYPE_INT,
	TYPE_STRING,
	TYPE_BETWEEN
};


class SelectParam{
private:
	std::shared_ptr<arrow::Table> dimTable;
	std::string select_field;
	SelectType sType;

public:
	void SetDimTable(std::shared_ptr<arrow::Table> _dimTable);
	std::shared_ptr<arrow::Table> GetDimTable();

	void SetSelectField(std::string _select_field);	
	std::string GetSelectField();

	void SetType(SelectType type);
	SelectType GetType();
	
	virtual void PrintParam() = 0;
};



class SelectParamInt : public SelectParam{
private:
	long long value;
	Operator op;
public:
	SelectParamInt(std::shared_ptr<arrow::Table> _dimTable, std::string _select_field, long long _value, Operator _op);
	long long GetValue();
	Operator GetOperator();
	void PrintParam();
};


class SelectParamString : public SelectParam{
private:
	std::string value;
	Operator op;
public:
	SelectParamString(std::shared_ptr<arrow::Table> _dimTable, std::string _select_field, std::string _value, Operator _op);
	std::string GetValue();
	Operator GetOperator();
	void PrintParam();
};

class SelectParamBetween : public SelectParam{
private:
	long long lo_value;
	long long hi_value;
public:
	SelectParamBetween(std::shared_ptr<arrow::Table> _dimTable, std::string _select_field, long long _lo_value, long long _hi_value);
	long long GetLoValue();
	long long GetHiValue();
	void PrintParam();
};




class JoinParam{
private:
	std::string fact_foreign_key;
	std::string dim_primary_key;

	SelectParam* sParam;
public:
	JoinParam(std::string _fact_foreign_key, std::string _dim_primary_key, SelectParam* _sParam);
	std::string GetFactForeignKey();
	std::string GetDimPrimaryKey();
	SelectParam* GetSelectParam();
	void PrintParam();
};

#endif