//
// Created by Nicholas Corrado on 11/13/19.
//

#ifndef CSV_TO_ARROW_CPP_UTIL_H
#define CSV_TO_ARROW_CPP_UTIL_H
#include <arrow/api.h>

void EvaluateStatus(arrow::Status status);
void PrintTable(std::shared_ptr<arrow::Table> table);


#endif //CSV_TO_ARROW_CPP_UTIL_H
