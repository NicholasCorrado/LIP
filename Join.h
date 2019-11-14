#ifndef JOIN_H
#define JOIN_H


#include <iostream>
#include <arrow/api.h>
#include "select.h"
#include <map>

std::shared_ptr<arrow::Table> HashJoin(std::shared_ptr<arrow::Table> left_table, std::string left_field, std::shared_ptr<arrow::Table> right_table, std::string right_field);



#endif