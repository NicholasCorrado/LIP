#include "Join.h"
#include "util/util.h"

std::vector<long long> HashJoin(std::shared_ptr<arrow::Table> left_table, std::string left_field, std::shared_ptr<arrow::Table> right_table, std::string right_field) {


    std::map<long long, bool> hash;


    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(arrow::compute::CompareOperator::EQUAL);
    auto* filter = new arrow::compute::Datum();

    auto* reader = new arrow::TableBatchReader(*left_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(left_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            hash[key] = true;
        }
    }


    std::vector<long long> ret;

    EvaluateStatus(status);
    reader = new arrow::TableBatchReader(*right_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(right_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            if ( hash.count(key) > 0 ) {
                ret.push_back(key);
            }
        }
    }
    
    return ret;
}

	