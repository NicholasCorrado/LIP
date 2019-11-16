#include "Join.h"
#include "util/util.h"

// As on 2019/11/15, we reutrn a table of tuples in left_table that would be joined with a tuple in right_table.
// Hence, the join result will never be larger than the size of the left_table.
std::shared_ptr<arrow::Table> HashJoin(std::shared_ptr<arrow::Table> left_table, std::string left_field, 
                                        std::shared_ptr<arrow::Table> right_table, std::string right_field) {


    std::map<long long, bool> hash;

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    auto* reader = new arrow::TableBatchReader(*right_table);


    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(right_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            hash[key] = true;
        }
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(left_table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status);

    reader = new arrow::TableBatchReader(*left_table);

    arrow::Int64Builder array_builder;


    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        // resize to maximum possible size to avoid resizing many times upon insertion.
        status = array_builder.Resize(in_batch->num_rows());
        EvaluateStatus(status);
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(left_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            if ( hash.count(key) > 0 ) {
                //@TODO: Keep the join implementation consistent!!!!!!!
                // In LIP join, we use an int array to hold the indices. So, hash join and LIP are implemented differently.
                // This causes issues when trying to compare performance between LIP and has join.
                status = array_builder.Append(i);
                EvaluateStatus(status);
                //AddRowToRecordBatch(i, in_batch, out_batch_builder);
            }
        }

        std::shared_ptr<arrow::Int64Array> indices_array;
        status = array_builder.Finish(&indices_array); // builders are automatically reset upon calling Finish()
        EvaluateStatus(status);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto* out = new arrow::compute::Datum();

        for (int k=0; k<in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context,in_batch->column(k),indices_array, take_options, out);
            EvaluateStatus(status);
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



std::shared_ptr<arrow::Table> EvaluateJoinTree(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors){
    
    std::shared_ptr<arrow::Table> ret_table = fact_table;

    for(int i = 0; i < joinExecutors.size(); i++){
        ret_table = joinExecutors[i] -> join(ret_table); // foreign key is a private var in joinExecutor
    }

    return ret_table;
}


std::shared_ptr<arrow::Table> EvaluateJoinTreeLIP(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors){
    
    int n_dim = joinExecutors.size();

    if (n_dim == 0){
        return fact_table;
    }
    // Construct the array of bloom filters
    std::vector<BloomFilter*> filters;

    for(int i = 0; i < n_dim; i++){
        BloomFilter* bf = joinExecutors[i] -> ConstructFilter();
        filters.push_back(bf);
    }
    
    // prepare to probe each fact
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(fact_table->schema(), arrow::default_memory_pool(), &out_batch_builder);

    auto* reader = new arrow::TableBatchReader(*fact_table);
    reader->set_chunksize(2 << 10);
    int* indices;
    
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        int n_rows = in_batch -> num_rows();

        indices = (int*)malloc(n_rows * sizeof(int));
        int index_size = n_rows;


        for(int i = 0; i < n_rows; i++){
            indices[i] = i;
        }

        for(int filter_index = 0; filter_index < n_dim; filter_index++){
            BloomFilter* bf_j = filters[filter_index];

            std::string foreign_key_j = bf_j -> GetForeignKey();

            auto col_j = std::static_pointer_cast<arrow::Int64Array>(
                    in_batch->GetColumnByName(foreign_key_j)
                    );


            int index_i = 0;

            while (index_i < index_size){
                int actual_index = indices[index_i];

                long long key_i = col_j -> Value(actual_index);

                bf_j -> IncrementCount();

                if ( !bf_j -> Search(key_i)) {
                    //AddRowToRecordBatch(i, in_batch, out_batch_builder);
                    // swap the position at index_i and index_size - 1;
                    int tmp = indices[index_i];
                    indices[index_i] = indices[index_size - 1];
                    indices[index_size - 1] = tmp;
                    index_size--;
                }
                else{
                    index_i++;
                    bf_j -> IncrementPass();
                }
            }
        }

        std::sort(filters.begin(), filters.end(), BloomFilterCompare);

        arrow::Int64Builder indices_builder;
        std::shared_ptr<arrow::Array> indices_array;
        status = indices_builder.AppendValues(indices,indices+index_size);
        EvaluateStatus(status);
        status = indices_builder.Finish(&indices_array);
        EvaluateStatus(status);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto* out = new arrow::compute::Datum();

        for (int k=0; k<in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context,in_batch->column(k),indices_array, take_options, out);
            EvaluateStatus(status);
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
    //return EvaluateJoinTree(result_table, joinExecutors);
}




	