#include "Join.h"
#include "util/util.h"

// As on 2019/11/15, we reutrn a table of tuples in left_table that would be joined with a tuple in right_table.
// Hence, the join result will never be larger than the size of the left_table.
std::shared_ptr<arrow::Table> HashJoin(std::shared_ptr<arrow::Table> left_table, std::string left_field, 
                                        std::shared_ptr<arrow::Table> right_table, std::string right_field) {


    std::map<long long, bool> hash;

    std::cout << "rows in left table = " << left_table -> num_rows() << std::endl;
    std::cout << "rows in right table = " << right_table -> num_rows() << std::endl;
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


    // Construct the array of bloom filters
    std::vector<BloomFilter*> filters;

    for(int i = 0; i < n_dim; i++){
        BloomFilter* bf = joinExecutors[i] -> ConstructFilter();
        filters.push_back(bf);
    }

    
    // prepare to probe each fact


    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;


    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    auto* reader = new arrow::TableBatchReader(*fact_table);


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

        for (int index_i = 0; index_i < index_size; index_i++) {
            int actual_index = indices[index_i];
            AddRowToRecordBatch(actual_index, in_batch, out_batch_builder);
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




	