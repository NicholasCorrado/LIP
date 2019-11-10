//
// Created by Nicholas Corrado on 10/29/19.
//

#ifndef CSV_TO_ARROW_CPP_MAIN_H
#define CSV_TO_ARROW_CPP_MAIN_H

#endif //CSV_TO_ARROW_CPP_MAIN_H

#include <arrow/api.h>

std::shared_ptr<arrow::Table> build_table(const std::string&, arrow::MemoryPool *, std::vector<std::string> & );
void write_to_file(std::string path, std::shared_ptr<arrow::Table> table);