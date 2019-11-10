//
// Created by Nicholas Corrado on 11/9/19.
//

#include <iostream>
#include <arrow/api.h>
#include "select.h"

// Currently only supports queries on columns with string data.
std::shared_ptr<arrow::Table>
Select(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, Operator op) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;

    status = arrow::RecordBatchBuilder::Make(table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status);
    auto* reader = new arrow::TableBatchReader(*table);

    // The Status outcome object returned by ReadNext() does NOT return an error when you are already at the final
    // record batch; it sets the output batch to nullptr and returns Status::OK(). Hence, we must also check that the
    // output batch is not nullptr.
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        int type = table->schema()->GetFieldByName(select_field)->type()->id();

        // Arrays must be downcasted to a concrete array type before we can access the array items. Note that the
        // method used to access item i of an array depend on the data type, e.g. GetString(i) for StringArray vs
        // Value(i) for Int64Array (and any other int array).
        switch (type) {
            case arrow::Type::type::STRING: {
                auto col = std::static_pointer_cast<arrow::StringArray>(in_batch->GetColumnByName(select_field));

                for (int i=0; i<col->length(); i++) {
                    if ( EvaluatePredicate(col->GetString(i), value, op) ) {
                        AddRowToRecordBatch(i, in_batch, out_batch_builder);
                    }
                }
                break;
            }
                /*
                case arrow::Type::type::INT64: {
                    auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(select_field));

                    for (int i=0; i<col->length(); i++) {
                        if ( EvaluatePredicate(col->Value(i), value, op) )) {
                            AddRowToRecordBatch(i, in_batch, out_batch_builder);
                        }
                    }
                    break;
                }
                */
        }
        std::shared_ptr<arrow::RecordBatch> out_batch;

        status = out_batch_builder->Flush(true, &out_batch);
        EvaluateStatus(status);

        out_batches.push_back(out_batch);
    }

    std::shared_ptr<arrow::Table> result_table;
    status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status);

    return result_table;
}

void AddRowToRecordBatch(int row, std::shared_ptr<arrow::RecordBatch>& in_batch, std::unique_ptr<arrow::RecordBatchBuilder>& out_batch_builder) {

    for (int i; i<out_batch_builder->schema()->num_fields(); i++) {
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

template <typename T>
bool EvaluatePredicate(T data, T value, Operator op) {

    bool result = false;

    switch(op) {
        case Operator::EQUAL:         result = (data == value); break;
        case Operator::LESS:          result = (data <  value); break;
        case Operator::LESS_EQUAL:    result = (data <= value); break;
        case Operator::GREATER:       result = (data >  value); break;
        case Operator::GREATER_EQUAL: result = (data >= value); break;
    }
    return result;
}

void EvaluateStatus(arrow::Status status) {
    if (!status.ok()) {
        std::cout << "ah" << std::endl;
    }
}

void PrintTable(std::shared_ptr<arrow::Table> table) {

    auto* reader = new arrow::TableBatchReader(*table);
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status status;

    status = reader->ReadNext(&batch);
    while (batch != nullptr) {
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