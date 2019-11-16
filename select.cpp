//
// Created by Nicholas Corrado on 11/9/19.
//

#include <iostream>
#include "select.h"
#include "util/util.h"


arrow::compute::Datum* GetSelectFilter(std::shared_ptr<arrow::RecordBatch> in_batch,
                                     std::string select_field,
                                     std::shared_ptr<arrow::Scalar> value,
                                     arrow::compute::CompareOperator op) {

    arrow::Status status;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(op);
    auto* filter = new arrow::compute::Datum();

    status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), value, compare_options, filter);
    EvaluateStatus(status);

    return filter;
}

// NOT should be handled separately, as it is a unary operator.
enum FilterCompareOperator {
    AND,
    OR,
    XOR,
};

arrow::compute::Datum* CombineSelectFilters(arrow::compute::Datum  &filter1,
                                               arrow::compute::Datum &filter2,
                                               FilterCompareOperator op) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto* filter = new arrow::compute::Datum();

    switch(op){
        case FilterCompareOperator::AND:
            status = arrow::compute::And(&function_context, filter1, filter2, filter);
            break;
        case FilterCompareOperator::OR:
            status = arrow::compute::Or(&function_context, filter1, filter2, filter);
            break;
        case FilterCompareOperator::XOR:
            status = arrow::compute::Xor(&function_context, filter1, filter2, filter);
            break;
    }
    EvaluateStatus(status);
    return filter;
}

arrow::compute::Datum* CombineSelectFilters(std::vector<arrow::compute::Datum*> filters,
                                            FilterCompareOperator op) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::Datum* filter = filters[0];

    switch(op){
        case FilterCompareOperator::AND:
            for (int i=1; i<filters.size(); i++) {
                status = arrow::compute::And(&function_context, *filter, *filters[i], filter);
                EvaluateStatus(status);
            }
            break;
        case FilterCompareOperator::OR:
            for (int i=1; i<filters.size(); i++) {
                status = arrow::compute::Or(&function_context, *filter, *filters[i], filter);
                EvaluateStatus(status);
            }
            break;
        case FilterCompareOperator::XOR:
            for (int i=1; i<filters.size(); i++) {
                status = arrow::compute::Xor(&function_context, *filter, *filters[i], filter);
                EvaluateStatus(status);
            }
            break;
    }

    return filter;
}

// Select with multiple predicates. Separate filters are combined in pairs.
std::shared_ptr<arrow::Table> SelectMany(std::shared_ptr<arrow::Table> table,
                                     std::vector<std::string> select_fields,
                                     std::vector<std::shared_ptr<arrow::Scalar>> values,
                                     std::vector<arrow::compute::CompareOperator> predicate_ops,
                                     std::vector<FilterCompareOperator> filter_ops) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status);

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto* filter_1 = new arrow::compute::Datum();
    auto* filter_2 = new arrow::compute::Datum();

    auto* reader = new arrow::TableBatchReader(*table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        for (int i=1; i<select_fields.size(); i++) {
            arrow::compute::CompareOptions compare_options_1(predicate_ops[i-1]);
            status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_fields[i]), values[i], compare_options_1, filter_1);
            arrow::compute::CompareOptions compare_options_2(predicate_ops[i]);
            status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_fields[i]), values[i], compare_options_2, filter_2);

            auto* out = new arrow::compute::Datum();

        }


    }


}

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

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }

    std::shared_ptr<arrow::Table> result_table;
    status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status);

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
// select queries on columns with string data if we really want to.
std::shared_ptr<arrow::Table> SelectString(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, arrow::compute::CompareOperator op) {

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

std::shared_ptr<arrow::Table> SelectStringBetween(std::shared_ptr<arrow::Table> table, std::string select_field, std::string lo, std::string hi) {

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
            std::string val = col->GetString(i);
            if ( lo <= val && val <= hi) {
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

template <typename T>
bool EvaluatePredicate(T data, T value, arrow::compute::CompareOperator op) {

    bool result = false;

    switch(op) {
        case arrow::compute::CompareOperator::EQUAL:         result = (data == value); break;
        case arrow::compute::CompareOperator::NOT_EQUAL:     result = (data >= value); break;
        case arrow::compute::CompareOperator::LESS:          result = (data <  value); break;
        case arrow::compute::CompareOperator::LESS_EQUAL:    result = (data <= value); break;
        case arrow::compute::CompareOperator::GREATER:       result = (data >  value); break;
        case arrow::compute::CompareOperator::GREATER_EQUAL: result = (data >= value); break;
    }
    return result;
}