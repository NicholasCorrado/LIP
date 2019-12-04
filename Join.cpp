#include "Join.h"
#include "util/util.h"
#include "util/sparsepp/spp.h"
#include <chrono>

#define DEBUG 1
#define CR 0
std::shared_ptr<arrow::Table> HashJoin(std::shared_ptr<arrow::Table> left_table, std::string left_field, 
                                        std::shared_ptr<arrow::Table> right_table, std::string right_field) {

    if (left_table == nullptr || right_table == nullptr
        || left_table -> num_rows() == 0 
        || right_table -> num_rows() == 0) return nullptr;

    spp::sparse_hash_map<long long, bool> hash(right_table->num_rows());

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    auto* reader = new arrow::TableBatchReader(*right_table);

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;


    start = std::chrono::high_resolution_clock::now();
    
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(right_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);
            hash[key] = true;
        }
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//    std::cout << "HashBuild " << right_field << " " << duration.count() << std::endl;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(left_table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    reader = new arrow::TableBatchReader(*left_table);

    arrow::Int64Builder array_builder;
    //@TODO array_builder should be initially resize to minimize the amount of resizing done during insertions.

    long long accu = 0;
    long long count = 0;


    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        // resize to maximum possible size to avoid resizing many times upon insertion.
        status = array_builder.Resize(in_batch->num_rows());
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(left_field));

        for (int i=0; i<col->length(); i++) {
            long long key = col -> Value(i);

            start = std::chrono::high_resolution_clock::now();
    
            int res = hash.count(key);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

            accu += duration.count();
            count++;
            if ( res > 0 ) {
                //@TODO: Keep the join implementation consistent!!!!!!!
                // In LIP join, we use an int array to hold the indices. So, hash join and LIP are implemented differently.
                // This causes issues when trying to compare performance between LIP and has join.
                status = array_builder.Append(i);
                EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
                //AddRowToRecordBatch(i, in_batch, out_batch_builder);
            }
        }

        std::shared_ptr<arrow::Int64Array> indices_array;
        status = array_builder.Finish(&indices_array); // builders are automatically reset upon calling Finish()
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto* out = new arrow::compute::Datum();

        for (int k=0; k<in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context,in_batch->column(k),indices_array, take_options, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }
//    std::cout << "PerHashProbe " << 1.0 * accu / count / 1000 << std::endl;
//    std::cout << "HashProbe " << accu << std::endl;

    std::shared_ptr<arrow::Table> result_table;
    status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    if (!status.ok()) return nullptr;
    return result_table;
}



std::shared_ptr<arrow::Table> EvaluateJoinTree(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors){
    
    std::shared_ptr<arrow::Table> ret_table = fact_table;
    std::shared_ptr<arrow::Table> tmp;


    for(int i = 0; i < joinExecutors.size(); i++){
        
        tmp = joinExecutors[i] -> join(ret_table); // foreign key is a private var in joinExecutor
        
        ret_table = tmp;
    }

    return ret_table;
}

std::shared_ptr<arrow::Table> EvaluateJoinTreeLIP(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors){

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;


    int n_dim = joinExecutors.size();

    if (n_dim == 0){
        return fact_table;
    }
    // Construct the array of bloom filters
    std::vector<BloomFilter*> filters;

//    start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < n_dim; i++){
        
        BloomFilter* bf = joinExecutors[i] -> ConstructBloomFilter();
        
        filters.push_back(bf);
    }
//    stop = std::chrono::high_resolution_clock::now();
//    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//    std::cout << "BuildFilters " <<  duration.count() << std::endl;
    
    // prepare to probe each fact
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    
    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(fact_table->schema(), arrow::default_memory_pool(), &out_batch_builder);

    auto* reader = new arrow::TableBatchReader(*fact_table);
//    reader->set_chunksize(2 << 13);
    int* indices;
//     long long accu = 0;
//     long long count = 0;

    int opt = 0;
    int alg = 0;


    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        int n_rows = in_batch -> num_rows();
        indices = (int*)malloc(n_rows * sizeof(int));
        int index_size = n_rows;

        for(int i = 0; i < n_rows; i++){
            indices[i] = i;
        }
//
//        for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
//            std::cout << filters[filter_index]->GetForeignKey() << " " << filters[filter_index]->GetFilterRate();
//        }
//        std::cout << std::endl;
//    std::cout << in_batch->num_rows() << std::endl;
        for(int filter_index = 0; filter_index < n_dim; filter_index++){
            BloomFilter* bf_j = filters[filter_index];

            std::string foreign_key_j = bf_j -> GetForeignKey();

            auto col_j = std::static_pointer_cast<arrow::Int64Array>(
                    in_batch->GetColumnByName(foreign_key_j) //fk in the fact table
                    );


            int index_i = 0;

            while (index_i < index_size){
                int actual_index = indices[index_i];

                long long key_i = col_j -> Value(actual_index);

                bf_j -> IncrementCount();

                if(CR) alg++;
//                start = std::chrono::high_resolution_clock::now();
//
//                stop = std::chrono::high_resolution_clock::now();
//                //duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//                duration = (stop - start);
//                accu += duration.count();
//                //std::cout << "duration = " << duration.count() << std::endl;
//                count++;

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
        
        if (CR) opt += (index_size) * (n_dim) + ( n_rows - index_size ) * 1;

        std::sort(filters.begin(), filters.end(), BloomFilterCompare);
//        filters = tmp;

        if (DEBUG) {
            for (int filter_index = 0; filter_index < n_dim; filter_index++) {
                std::cout << filters[filter_index]->GetForeignKey() << " " << filters[filter_index]->GetFilterRate() << " ";
            }
            std::cout << std::endl;
        }

        arrow::Int64Builder indices_builder;
        std::shared_ptr<arrow::Array> indices_array;
        status = indices_builder.AppendValues(indices,indices+index_size);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        status = indices_builder.Finish(&indices_array);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto* out = new arrow::compute::Datum();

        for (int k=0; k<in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context,in_batch->column(k),indices_array, take_options, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }

    std::shared_ptr<arrow::Table> result_table;

    if(CR){
        double cr = opt == 0 ? 1 : 1.0 * alg / opt;
        std::cout << "CR = " << alg << " / " << opt << " = "<<  cr << std::endl;
    }
    if (out_batches.size() > 0)
        status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    //return result_table;
//    std::cout << "PerFilterProbe " << 1.0 * accu / count / 1000 << std::endl;
//    std::cout << "FilterProbe " << accu / 1000 << std::endl;
    return EvaluateJoinTree(result_table, joinExecutors);
}


std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPXiating(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors){
    
    int n_dim = joinExecutors.size();

    if (n_dim == 0){
        return fact_table;
    }
    // Construct the array of bloom filters
    std::vector<BloomFilter*> filters;

    for(int i = 0; i < n_dim; i++){
        
        BloomFilter* bf = joinExecutors[i] -> ConstructBloomFilter();
        
        filters.push_back(bf);
    }
    // long long chunk_size = 2 << 10;

    // prepare to probe each fact
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    
    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(fact_table->schema(), arrow::default_memory_pool(), &out_batch_builder);

    auto* reader = new arrow::TableBatchReader(*fact_table);
    // reader->set_chunksize(chunk_size);
    int* indices;

    int opt = 0;
    int alg = 0;


    int cutoff = filters.size()-1;

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        // Note that we
        int n_rows = in_batch->num_rows();
        indices = (int *) malloc(n_rows * sizeof(int));
        int index_size = n_rows;


        for (int i = 0; i < n_rows; i++) {
            indices[i] = i;
        }
        double prev_filter_rate = 0;
        double selectivity = 1;
        for (int filter_index = 0; filter_index < n_dim && filter_index<=cutoff; filter_index++) {
            BloomFilter *bf_j = filters[filter_index];
            selectivity = selectivity*bf_j->GetFilterRate();

            bf_j->Reset();
            std::string foreign_key_j = bf_j->GetForeignKey();

            auto col_j = std::static_pointer_cast<arrow::Int64Array>(
                    in_batch->GetColumnByName(foreign_key_j)
            );


            int index_i = 0;

            while (index_i < index_size) {
                int actual_index = indices[index_i];

                long long key_i = col_j->Value(actual_index);

                bf_j->IncrementCount();
                alg ++;
                if (!bf_j->Search(key_i)) {
                    //AddRowToRecordBatch(i, in_batch, out_batch_builder);
                    // swap the position at index_i and index_size - 1;
                    int tmp = indices[index_i];
                    indices[index_i] = indices[index_size - 1];
                    indices[index_size - 1] = tmp;
                    index_size--;
                } else {
                    index_i++;
                    bf_j->IncrementPass();
                }
            }

            if (selectivity < 0.001) {
                //std::cout<<"selectivity = " << cutoff << std::endl;
                cutoff = filter_index;
            }
        }


        if (CR) opt += (index_size) * (cutoff) + ( n_rows - index_size ) * 1;

        std::sort(filters.begin(), filters.end(), BloomFilterCompare);
        /*
        for(int p = 0; p < n_dim; p++){
            std::cout << filters[p] -> GetFilterRate() << " ";
        }
        std::cout << std::endl;
        */
        arrow::Int64Builder indices_builder;
        std::shared_ptr<arrow::Array> indices_array;
        status = indices_builder.AppendValues(indices, indices + index_size);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        status = indices_builder.Finish(&indices_array);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto *out = new arrow::compute::Datum();

        for (int k = 0; k < in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context, in_batch->column(k), indices_array, take_options, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(), out_arrays[0]->length, out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }


    std::shared_ptr<arrow::Table> result_table;
    if (out_batches.size() > 0)
        status = arrow::Table::FromRecordBatches(out_batches, &result_table);

    if(CR){
        double cr = opt == 0 ? 1 : 1.0 * alg / opt;
        std::cout << "CR = " << alg << " / " << opt << " = "<<  cr << std::endl;
    }
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    //return result_table;
    return EvaluateJoinTree(result_table, joinExecutors);
}



std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPResurrection(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors){
    
    int n_dim = joinExecutors.size();

    if (n_dim == 0){
        return fact_table;
    }
    // Construct the array of bloom filters
    std::vector<BloomFilter*> filters;

    for(int i = 0; i < n_dim; i++){
        
        BloomFilter* bf = joinExecutors[i] -> ConstructBloomFilter();
        
        filters.push_back(bf);
    }
    // long long chunk_size = 2 << 10;

    // prepare to probe each fact
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    
    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(fact_table->schema(), arrow::default_memory_pool(), &out_batch_builder);

    auto* reader = new arrow::TableBatchReader(*fact_table);
    // reader->set_chunksize(chunk_size);
    int* indices;

    int opt = 0;
    int alg = 0;


    int cutoff = filters.size()-1;

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        // Note that we
        int n_rows = in_batch->num_rows();
        indices = (int *) malloc(n_rows * sizeof(int));
        int index_size = n_rows;

        for (int i = 0; i < n_rows; i++) {
            indices[i] = i;
        }
        double prev_filter_rate = 0;
        double selectivity = 1;
        for (int filter_index = 0; filter_index < n_dim && filter_index<=cutoff; filter_index++) {
            BloomFilter *bf_j = filters[filter_index];
            selectivity = selectivity*bf_j->GetFilterRate();

            bf_j->Reset();
            std::string foreign_key_j = bf_j->GetForeignKey();

            auto col_j = std::static_pointer_cast<arrow::Int64Array>(
                    in_batch->GetColumnByName(foreign_key_j)
            );


            int index_i = 0;

            while (index_i < index_size) {
                int actual_index = indices[index_i];

                long long key_i = col_j->Value(actual_index);

                bf_j->IncrementCount();

                if (CR) alg++;
                if (!bf_j->Search(key_i)) {
                    //AddRowToRecordBatch(i, in_batch, out_batch_builder);
                    // swap the position at index_i and index_size - 1;
                    int tmp = indices[index_i];
                    indices[index_i] = indices[index_size - 1];
                    indices[index_size - 1] = tmp;
                    index_size--;
                } else {
                    index_i++;
                    bf_j->IncrementPass();
                }
            }

            if (selectivity < 0.001) {
                //std::cout<<"selectivity = " << cutoff << std::endl;
                cutoff = filter_index;
            }
            else{
                cutoff = filters.size() - 1;
            }
        }
        if (CR) opt += (index_size) * (cutoff) + ( n_rows - index_size ) * 1;

        std::sort(filters.begin(), filters.end(), BloomFilterCompare);
        /*
        for(int p = 0; p < n_dim; p++){
            std::cout << filters[p] -> GetFilterRate() << " ";
        }
        std::cout << std::endl;
        */
        arrow::Int64Builder indices_builder;
        std::shared_ptr<arrow::Array> indices_array;
        status = indices_builder.AppendValues(indices, indices + index_size);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        status = indices_builder.Finish(&indices_array);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto *out = new arrow::compute::Datum();

        for (int k = 0; k < in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context, in_batch->column(k), indices_array, take_options, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(), out_arrays[0]->length, out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }


    std::shared_ptr<arrow::Table> result_table;
    if (out_batches.size() > 0)
        status = arrow::Table::FromRecordBatches(out_batches, &result_table);

    if(CR){
        double cr = opt == 0 ? 1 : 1.0 * alg / opt;
        std::cout << "CR = " << alg << " / " << opt << " = "<<  cr << std::endl;
    }
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    //return result_table;
    return EvaluateJoinTree(result_table, joinExecutors);
}


std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPK(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors, int k){


    int n_dim = joinExecutors.size();

    if (n_dim == 0){
        return fact_table;
    }
    // Construct the array of bloom filters
    std::vector<BloomFilter*> filters;

    for(int i = 0; i < n_dim; i++){
        BloomFilter* bf = joinExecutors[i] -> ConstructBloomFilter();
        bf -> SetMemory(k);
        filters.push_back(bf);
    }
    // long long chunk_size = 2 << 10;

    // prepare to probe each fact
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    
    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      
        // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   
        // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   
        // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(fact_table->schema(), arrow::default_memory_pool(), &out_batch_builder);

    auto* reader = new arrow::TableBatchReader(*fact_table);

    int* indices;

    int opt = 0;
    int alg = 0;


    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        int n_rows = in_batch->num_rows();
        indices = (int *) malloc(n_rows * sizeof(int));
        int index_size = n_rows;

//        for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
//            std::cout << filters[filter_index]->GetForeignKey() << " " << filters[filter_index]->GetFilterRateK() << " ";
//        }
//        std::cout << std::endl;

        for (int i = 0; i < n_rows; i++) {
            indices[i] = i;
        }
        double prev_filter_rate = 0;
        //double selectivity = 1;

        

        for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
            BloomFilter *bf_j = filters[filter_index];
            //selectivity = selectivity*bf_j->GetFilterRate();
            //selectivity = selectivity*bf_j->GetFilterRateK();
            std::string foreign_key_j = bf_j->GetForeignKey();

            auto col_j = std::static_pointer_cast<arrow::Int64Array>(
                    in_batch->GetColumnByName(foreign_key_j)
            );


            int index_i = 0;

            while (index_i < index_size) {
                int actual_index = indices[index_i];

                long long key_i = col_j->Value(actual_index);

                bf_j->IncrementCount();

                if (CR) alg ++;
                if (!bf_j->Search(key_i)) {
                    // swap the position at index_i and index_size - 1;
                    int tmp = indices[index_i];
                    indices[index_i] = indices[index_size - 1];
                    indices[index_size - 1] = tmp;
                    index_size--;
                } else {
                    index_i++;
                    bf_j->IncrementPass();
                }
            }
        }


        for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
            filters[filter_index] -> BatchEndUpdate();
        }
        if (CR) opt += (index_size) * (n_dim) + ( n_rows - index_size ) * 1;

        std::sort(filters.begin(), filters.end(), BloomFilterCompareK);

        if (DEBUG) {
            for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
                std::cout << filters[filter_index]->GetForeignKey() << " " << filters[filter_index]->GetFilterRateK();
            }
            std::cout << std::endl;
        }

        /*
        for(int p = 0; p < n_dim; p++){
            std::cout << filters[p] -> GetFilterRate() << " ";
        }
        std::cout << std::endl;
        */
        arrow::Int64Builder indices_builder;
        std::shared_ptr<arrow::Array> indices_array;
        status = indices_builder.AppendValues(indices, indices + index_size);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        status = indices_builder.Finish(&indices_array);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto *out = new arrow::compute::Datum();

        for (int j = 0; j < in_batch->schema()->num_fields(); j++) {
            status = arrow::compute::Take(&function_context, in_batch->column(j), indices_array, take_options, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(), out_arrays[0]->length, out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }


    std::shared_ptr<arrow::Table> result_table;
    if (out_batches.size() > 0)
        status = arrow::Table::FromRecordBatches(out_batches, &result_table);

    if(CR){
        double cr = opt == 0 ? 1 : 1.0 * alg / opt;
        std::cout << "CR = " << alg << " / " << opt << " = "<<  cr << std::endl;
    }
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    //return result_table;
    return EvaluateJoinTree(result_table, joinExecutors);
}


std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPTime(std::shared_ptr<arrow::Table> fact_table,
                                                   std::vector<JoinExecutor*> joinExecutors){


    int n_dim = joinExecutors.size();

    if (n_dim == 0){
        return fact_table;
    }

    std::vector<BloomFilter*> filters;

    for(int i = 0; i < n_dim; i++){
        BloomFilter* bf = joinExecutors[i] -> ConstructBloomFilter();
        //bf -> SetMemory(2);
        filters.push_back(bf);
    }

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;


    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;

    status = arrow::RecordBatchBuilder::Make(fact_table->schema(), arrow::default_memory_pool(), &out_batch_builder);

    auto* reader = new arrow::TableBatchReader(*fact_table);

    int* indices;

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        int n_rows = in_batch->num_rows();
        indices = (int *) malloc(n_rows * sizeof(int));
        int index_size = n_rows;

//        for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
//            std::cout << filters[filter_index]->GetForeignKey() << " " << filters[filter_index]->GetFilterRateK();
//        }
//        std::cout << std::endl;
        for (int i = 0; i < n_rows; i++) {
            indices[i] = i;
        }

        for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
            BloomFilter *bf_j = filters[filter_index];

            std::string foreign_key_j = bf_j->GetForeignKey();

            auto col_j = std::static_pointer_cast<arrow::Int64Array>(
                    in_batch->GetColumnByName(foreign_key_j)
            );


            int index_i = 0;

            start = std::chrono::high_resolution_clock::now();

            while (index_i < index_size) {
                int actual_index = indices[index_i];

                long long key_i = col_j->Value(actual_index);

                bf_j->IncrementCount();

                if (!bf_j->Search(key_i)) {
                    // swap the position at index_i and index_size - 1;
                    int tmp = indices[index_i];
                    indices[index_i] = indices[index_size - 1];
                    indices[index_size - 1] = tmp;
                    index_size--;
                } else {
                    index_i++;
//                    bf_j->IncrementPass();
                }
            }

            bf_j->time += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start).count();
        }

        std::sort(filters.begin(), filters.end(), BloomFilterCompareTime);

        if (DEBUG) {
            for (int filter_index = 0; filter_index < n_dim ; filter_index++) {
                if (filters[filter_index]->count > 0)
                    std::cout << filters[filter_index]->GetForeignKey() << " " << filters[filter_index]->time/filters[filter_index]->count << " ";
            }
            std::cout << std::endl;
        }

        /*
        for(int p = 0; p < n_dim; p++){
            std::cout << filters[p] -> GetFilterRate() << " ";
        }
        std::cout << std::endl;
        */
        arrow::Int64Builder indices_builder;
        std::shared_ptr<arrow::Array> indices_array;
        status = indices_builder.AppendValues(indices, indices + index_size);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        status = indices_builder.Finish(&indices_array);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

        // Instantiate things needed for a call to Take()
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        auto *out = new arrow::compute::Datum();

        for (int k = 0; k < in_batch->schema()->num_fields(); k++) {
            status = arrow::compute::Take(&function_context, in_batch->column(k), indices_array, take_options, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(), out_arrays[0]->length, out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }


    std::shared_ptr<arrow::Table> result_table;
    if (out_batches.size() > 0)
        status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    //return result_table;
    return EvaluateJoinTree(result_table, joinExecutors);
}
