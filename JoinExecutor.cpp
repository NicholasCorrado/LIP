#include "JoinExecutor.h"
#include "select.h"
#include "Join.h"
#include "BuildFilter.h"
#include "util/util.h"


std::shared_ptr<arrow::RecordBatch>
SelectExecutor::select(std::shared_ptr<arrow::RecordBatch> in_batch){

	arrow::compute::Datum* filter = GetBitFilter(in_batch);
	std::shared_ptr<arrow::Table> ret;

	/*

		filter the result table here

	*/
    arrow::Status status;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays

    status = arrow::RecordBatchBuilder::Make(in_batch->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto* out = new arrow::compute::Datum();

    for (int i=0; i<in_batch->schema()->num_fields(); i++) {
        status = arrow::compute::Filter(&function_context, in_batch->column(i), *filter, out);
        out_arrays.push_back(out->array());
    }

    auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);

	return out_batch;
}



BloomFilter* 
SelectExecutorTree::ConstructBloomFilterNoFK(std::string dim_primary_key){

    arrow::Status status;
	arrow::compute::Datum* filter;// = GetBitFilter();
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    std::shared_ptr<arrow::RecordBatch> in_batch;
    auto* reader = new arrow::TableBatchReader(*dim_table);

    //auto start = std::chrono::high_resolution_clock::now();
    BloomFilter* bf = new BloomFilter(dim_table->num_rows()/2); //estimate selectivity = 0.5; horrible

    

    if (root == nullptr){
        while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
            // NICK HELP:
            // NEED A NCIER WAY THAT WHEN ROOT IS NULL, JUST INSERT THE KEYS
            //@TODO Check that this works
            std::shared_ptr<arrow::Int64Array> keys = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(dim_primary_key));

            for (int i=0;i<keys->length(); i++) {
                bf->Insert(keys->Value(i));
            }
        }
    }
    else{

        while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
            
            filter = root->GetBitFilter(in_batch);
            
            auto* out = new arrow::compute::Datum();
            
            status = arrow::compute::Filter(&function_context, in_batch->GetColumnByName(dim_primary_key), *filter, out);
            EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
            
            
            std::shared_ptr<arrow::Int64Array> keys = std::static_pointer_cast<arrow::Int64Array>(out->make_array());

            for (int i=0;i<keys->length(); i++) {
                bf->Insert(keys->Value(i));
            }
        }
    
    }

    // auto stop = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    
    // std::cout << "BloomBuild " << duration.count() << std::endl;

    return bf;

}
	


SelectExecutorTree::SelectExecutorTree(std::shared_ptr<arrow::Table> _dim_table, SelectExecutor* _root) {
    dim_table = _dim_table;
    root = _root;
}

std::shared_ptr<arrow::Table> SelectExecutorTree::Select() {
    
    if (root == nullptr) 
        return dim_table;
    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    std::shared_ptr<arrow::RecordBatch> in_batch;
    auto* out = new arrow::compute::Datum();

    auto* reader = new arrow::TableBatchReader(*dim_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        arrow::compute::Datum* filter = root->GetBitFilter(in_batch);
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
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    
    return result_table;
}


SelectExecutorComposite::SelectExecutorComposite(std::vector<SelectExecutor*> _children){

    //dim_table = _children[0]->dim_table; // assuming that all child select exe have the same dim_table
	children = _children;
}


arrow::compute::Datum* 
SelectExecutorComposite::GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch){
	int num_of_children = children.size();
	arrow::Status status;
	arrow::BooleanBuilder boolean_builder;
	std::shared_ptr<arrow::BooleanArray> boolean_array;

	status = boolean_builder.AppendValues(in_batch->num_rows(), false);
	EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
	status = boolean_builder.Finish(&boolean_array);
	EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto* filter = new arrow::compute::Datum(boolean_array);

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto* tmp = new arrow::compute::Datum(); /*initialize it to all 0*/

	for(int child_i = 0; child_i < num_of_children; child_i++){
		arrow::compute::Datum* child_filter = children[child_i] -> GetBitFilter(in_batch);

		status = arrow::compute::Or(&function_context, *filter, *child_filter, tmp);
		EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

		filter = tmp;

		/* ret = ret || child_filter */
	}
	return filter;
}





SelectExecutorInt::SelectExecutorInt(std::string _select_field,
                                            long long _value,
                                            arrow::compute::CompareOperator _op){

	select_field = _select_field;
	arrow::NumericScalar<arrow::Int64Type> myscalar(_value);
    value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);
	op = _op;
}



arrow::compute::Datum* 
SelectExecutorInt::GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch){

	/* Get bit filter satisfying (integers op value) */

    arrow::Status status;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(op);
    auto* filter = new arrow::compute::Datum();

    status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), value, compare_options, filter);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    return filter;
}



SelectExecutorStr::SelectExecutorStr( std::string _select_field,
                                        std::string _value,
                                        arrow::compute::CompareOperator _op){
	//dim_table = _dim_table;
	select_field = _select_field;

	value = _value;
	op = _op;
}


arrow::compute::Datum* 
SelectExecutorStr::GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch){


    arrow::Status status;

    arrow::BooleanBuilder boolean_builder;
    std::shared_ptr<arrow::BooleanArray> boolean_array;

    status = boolean_builder.Resize(in_batch->num_rows());
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto col = std::static_pointer_cast<arrow::StringArray>(in_batch->GetColumnByName(select_field));//we guarantee that there is only one chunk

    for (int i=0; i<col->length(); i++) {
        status = boolean_builder.Append(EvaluatePredicate(col->GetString(i), value, op));
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    }

    status = boolean_builder.Finish(&boolean_array);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto* filter = new arrow::compute::Datum(boolean_array);

	return filter;

}



SelectExecutorBetween::SelectExecutorBetween(
												std::string _select_field, 
												long long _lo_value, 
												long long _hi_value){
	//dim_table = _dim_table;
	select_field = _select_field;

	arrow::NumericScalar<arrow::Int64Type> lo_scalar(_lo_value);
    lo_value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(lo_scalar);
	
	arrow::NumericScalar<arrow::Int64Type> hi_scalar(_hi_value);
    hi_value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(hi_scalar);
}


arrow::compute::Datum* 
SelectExecutorBetween::GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch){
	arrow::compute::Datum* ret;
	/* Get bit filter satisfying (lo <= integers <= hi) */

    arrow::Status status;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options_leq(arrow::compute::CompareOperator::LESS_EQUAL);
    arrow::compute::CompareOptions compare_options_geq(arrow::compute::CompareOperator::GREATER_EQUAL);
    auto* filter_leq = new arrow::compute::Datum();
    auto* filter_geq = new arrow::compute::Datum();
    auto* filter = new arrow::compute::Datum();

    status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), lo_value, compare_options_geq, filter_geq); // bool array
    status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), hi_value, compare_options_leq, filter_leq); // bool array
    status = arrow::compute::And(&function_context,*filter_leq, *filter_geq, filter);

    return filter;
}

SelectExecutorStrBetween::SelectExecutorStrBetween(
                                                   std::string _select_field,
                                                   std::string _lo_value,
                                                   std::string _hi_value) {
    //dim_table = _dim_table;
    select_field = _select_field;

    lo_value = _lo_value;
    hi_value = _hi_value;
}

arrow::compute::Datum*
SelectExecutorStrBetween::GetBitFilter(std::shared_ptr<arrow::RecordBatch> in_batch){
    arrow::compute::Datum* ret;
    /* Get bit filter satisfying (lo <= strings <= hi) */
    arrow::Status status;

    arrow::BooleanBuilder boolean_builder;
    std::shared_ptr<arrow::BooleanArray> boolean_array;

    status = boolean_builder.Resize(in_batch->num_rows());
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto col = std::static_pointer_cast<arrow::StringArray>(in_batch->GetColumnByName(select_field));//we guarantee that there is only one chunk

    for (int i=0; i<col->length(); i++) {
        status = boolean_builder.Append(
                EvaluatePredicate(col->GetString(i), lo_value, arrow::compute::CompareOperator::GREATER_EQUAL) &&
                EvaluatePredicate(col->GetString(i), hi_value, arrow::compute::CompareOperator::LESS_EQUAL));
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    }

    status = boolean_builder.Finish(&boolean_array);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto* filter = new arrow::compute::Datum(boolean_array);
    return filter;
}


JoinExecutor::JoinExecutor(SelectExecutorTree* _s_tree_exe,
                           std::string _dim_primary_key,
                           std::string _fact_foreign_key){
    fact_foreign_key = _fact_foreign_key;
    dim_primary_key = _dim_primary_key;
    select_tree_exe = _s_tree_exe;
}

std::shared_ptr<arrow::Table> JoinExecutor::join(std::shared_ptr<arrow::Table> fact_table){
    
    arrow::Status status;
    std::shared_ptr<arrow::Table> dim_table_selected;

	if (select_tree_exe->root == nullptr) {
	    dim_table_selected = select_tree_exe->dim_table;
	}
	else {
        dim_table_selected = select_tree_exe ->Select();
	}

	std::shared_ptr<arrow::Table> ret;
    ret = HashJoin(fact_table, fact_foreign_key, dim_table_selected, dim_primary_key);
    
	return ret;
}



BloomFilter* JoinExecutor::ConstructBloomFilter(){
    BloomFilter* bf = select_tree_exe -> ConstructBloomFilterNoFK(dim_primary_key);
	
    bf -> SetForeignKey(fact_foreign_key);
	return bf;
}

