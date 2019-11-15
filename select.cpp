//
// Created by Nicholas Corrado on 11/9/19.
//

#include <iostream>
#include "select.h"
#include "util/util.h"

std::shared_ptr<arrow::Table> Select(std::shared_ptr<arrow::Table> table, 
                                        std::string select_field, 
                                        std::shared_ptr<arrow::Scalar> value, 
                                        arrow::compute::CompareOperator op) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status);

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(op);
    auto* filter = new arrow::compute::Datum();

    auto* reader = new arrow::TableBatchReader(*table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), value, compare_options, filter);

        auto* out = new arrow::compute::Datum();

        // For each column, store all values that pass the filter
        for (int i=0; i<in_batch->schema()->num_fields(); i++) {
            status = arrow::compute::Filter(&function_context, in_batch->column(i), *filter, out);
            out_arrays.push_back(out->array());
        }
        std::cout << "out_arrays = " << out_arrays[0]->length << std::endl;
        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }

    std::shared_ptr<arrow::Table> result_table;
    status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status);
    std::cout << "select table = " << result_table->num_rows() << std::endl;
    return result_table;
}


std::shared_ptr<arrow::Table> SelectBetween(std::shared_ptr<arrow::Table> table, 
                                                std::string select_field, 
                                                std::shared_ptr<arrow::Scalar> lo, 
                                                std::shared_ptr<arrow::Scalar> hi){



    std::shared_ptr<arrow::Table> result_table = table;

    arrow::compute::CompareOperator geq = arrow::compute::CompareOperator::GREATER_EQUAL;
    result_table = Select(result_table, select_field, lo, geq);

    arrow::compute::CompareOperator leq = arrow::compute::CompareOperator::LESS_EQUAL;
    result_table = Select(result_table, select_field, hi, leq);

    return result_table;
}



// Arrow currently does not support vectorized string comparison. Nevertheless, we can still use this function to run
// select queries on columns with string data if we really wanted to.
/*
std::shared_ptr<arrow::Table> SelectString(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, Operator op) {

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

        // Arrays must be downcasted to a concrete array type before we can access the array items. Note that the
        // method used to access item i of an array depend on the data type, e.g. GetString(i) for StringArray vs
        // Value(i) for Int64Array (and any other int array).
        auto col = std::static_pointer_cast<arrow::StringArray>(in_batch->GetColumnByName(select_field));

        for (int i=0; i<col->length(); i++) {
            if ( EvaluatePredicate(col->GetString(i), value, op) ) {
                AddRowToRecordBatch(i, in_batch, out_batch_builder);
            }
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
*/