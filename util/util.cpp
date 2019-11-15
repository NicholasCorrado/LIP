//
// Created by Nicholas Corrado on 11/13/19.
//

#include <iostream>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/csv/api.h>
#include "util.h"


void write_to_file(const char* path, std::shared_ptr<arrow::Table> &table) {

    std::shared_ptr<arrow::io::FileOutputStream> file;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
    arrow::Status status;

    status = arrow::io::FileOutputStream::Open(path,&file);
    EvaluateStatus(status);

    status = arrow::ipc::RecordBatchFileWriter::Open(&*file, table->schema()).status();

    if (status.ok()) {
        record_batch_writer = arrow::ipc::RecordBatchFileWriter::Open(&*file, table->schema()).ValueOrDie();
    }
    status = record_batch_writer->WriteTable(*table);
    EvaluateStatus(status);

    status = record_batch_writer->Close();
    EvaluateStatus(status);
}

std::shared_ptr<arrow::Table>
        build_table(const std::string& file_path, arrow::MemoryPool *pool, std::vector<std::string> &schema) {

    arrow::Status status;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    status = arrow::io::ReadableFile::Open(file_path, pool, &infile);
    EvaluateStatus(status);

    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.column_names = schema;
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::shared_ptr<arrow::csv::TableReader> reader;
    status = arrow::csv::TableReader::Make(arrow::default_memory_pool(), infile, read_options, parse_options, convert_options, &reader);
    EvaluateStatus(status);

    std::shared_ptr<arrow::Table> table;
    status = reader->Read(&table);
    EvaluateStatus(status);

    return table;
}


void EvaluateStatus(arrow::Status status) {
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
}
void PrintTable(std::shared_ptr<arrow::Table> table) {

    auto* reader = new arrow::TableBatchReader(*table);
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status status;

    while (reader->ReadNext(&batch).ok() && batch != nullptr) {
        for (int row=0; row < batch->num_rows(); row++) {
            for (int i = 0; i < batch->schema()->num_fields(); i++) {

                int type = batch->schema()->field(i)->type()->id();

                switch (type) {
                    case arrow::Type::type::STRING: {
                        auto col = std::static_pointer_cast<arrow::StringArray>(batch->column(i));
                        std::cout << col->GetString(row) << "\t";
                        break;
                    }
                    case arrow::Type::type::INT64: {
                        auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(i));
                        std::cout << col->Value(row) << "\t";
                        break;
                    }
                }
            }
            std::cout << std::endl;
        }
        status = reader->ReadNext(&batch);
    }
}

void AddRowToRecordBatch(int row, std::shared_ptr<arrow::RecordBatch>& in_batch, std::unique_ptr<arrow::RecordBatchBuilder>& out_batch_builder) {

    for (int i = 0; i<out_batch_builder->schema()->num_fields(); i++) {
        arrow::ArrayBuilder* builder = out_batch_builder->GetField(i);
        int type = out_batch_builder->schema()->field(i)->type()->id();

        switch (type) {
            case arrow::Type::type::STRING: {
                auto* type_builder = dynamic_cast<arrow::StringBuilder *>(builder);
                auto in_col = std::static_pointer_cast<arrow::StringArray>(in_batch->column(i));
                type_builder->Append(in_col->GetString(row));
                break;
            }
            case arrow::Type::type::INT64: {
                auto* type_builder = dynamic_cast<arrow::Int64Builder *>(builder);
                auto in_col = std::static_pointer_cast<arrow::Int64Array>(in_batch->column(i));
                type_builder->Append(in_col->Value(row));
                break;
            }
        }
    }
}


