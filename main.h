//
// Created by Nicholas Corrado on 10/29/19.
//

#ifndef MAIN_H
#define MAIN_H


#include <arrow/api.h>

std::shared_ptr<arrow::Table> build_table(const std::string&, arrow::MemoryPool *, std::vector<std::string> & );
void write_to_file(const char* path, std::shared_ptr<arrow::Table> &table);


#endif