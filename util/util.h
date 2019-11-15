//
// Created by Nicholas Corrado on 11/13/19.
//

#ifndef CSV_TO_ARROW_CPP_UTIL_H
#define CSV_TO_ARROW_CPP_UTIL_H
#include <arrow/api.h>

void EvaluateStatus(arrow::Status status);
void PrintTable(std::shared_ptr<arrow::Table> table);
void AddRowToRecordBatch(int row, std::shared_ptr<arrow::RecordBatch>& in_batch, std::unique_ptr<arrow::RecordBatchBuilder>& out_batch_builder);


#endif //CSV_TO_ARROW_CPP_UTIL_H
