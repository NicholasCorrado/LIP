//
// Created by Nicholas Corrado on 11/13/19.
//

#ifndef CSV_TO_ARROW_CPP_UTIL_H
#define CSV_TO_ARROW_CPP_UTIL_H
#include <arrow/api.h>

void EvaluateStatus(const arrow::Status& status, const char* function_name, int line_no);
void PrintTable(std::shared_ptr<arrow::Table> table);
void AddRowToRecordBatch(int row, std::shared_ptr<arrow::RecordBatch>& in_batch, std::unique_ptr<arrow::RecordBatchBuilder>& out_batch_builder);
void write_to_file(const char* path, std::shared_ptr<arrow::Table> &table);
std::shared_ptr<arrow::Table> build_table(const std::string& file_path, arrow::MemoryPool *pool, std::vector<std::string> &schema);

#endif //CSV_TO_ARROW_CPP_UTIL_H
