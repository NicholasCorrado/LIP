//
// Created by Nicholas Corrado on 10/29/19.
//

#ifndef CSV_TO_ARROW_CPP_MAIN_H
#define CSV_TO_ARROW_CPP_MAIN_H

#endif //CSV_TO_ARROW_CPP_MAIN_H

#include <cstdlib>
#include <arrow/api.h>


std::shared_ptr<arrow::Table> build_table(const std::string&, arrow::MemoryPool *, std::vector<std::string> & );
std::shared_ptr<arrow::Array> get_row(std::shared_ptr<arrow::Table> table, uint row_i);
void select1(std::shared_ptr<arrow::Table>, std::string , std::vector<std::string> );
void select(std::shared_ptr<arrow::Table>, std::string, std::string );