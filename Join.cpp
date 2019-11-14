#include "Join.h"
#include "select.h"


std::shared_ptr<arrow::Table> HashJoin(std::shared_ptr<arrow::Table> left_table, std::string left_field, std::shared_ptr<arrow::Table> right_table, std::string right_field) {


    std::map<long long, bool> hash;

    std::cout << left_table -> num_rows() << std::endl;
    std::cout << right_table -> num_rows() << std::endl;
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    
    EvaluateStatus(status);
    auto* reader = new arrow::TableBatchReader(*right_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(right_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            hash[key] = true;
        }
    }


    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;

    status = arrow::RecordBatchBuilder::Make(left_table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status);

    reader = new arrow::TableBatchReader(*left_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(left_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            if ( hash.count(key) > 0 ) {
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

	