
#include "JoinExecutor.h"
#include "select.h"
#include "Join.h"
#include "BuildFilter.h"
#include "util/util.h"


std::shared_ptr<arrow::Table> 
SelectExecutor::select(){

	arrow::compute::Datum* filter = GetBitFilter();
	std::shared_ptr<arrow::Table> ret;

	/*

		filter the result table here

	*/
    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;   // output table will be built from a vector of RecordBatches

    status = arrow::RecordBatchBuilder::Make(dim_table->schema(), arrow::default_memory_pool(), &out_batch_builder);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    auto* out = new arrow::compute::Datum();

    auto* reader = new arrow::TableBatchReader(*dim_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        for (int i=0; i<in_batch->schema()->num_fields(); i++) {
            status = arrow::compute::Filter(&function_context, in_batch->column(i), *filter, out);
            out_arrays.push_back(out->array());
        }

        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);
        out_batches.push_back(out_batch);

        out_arrays.clear();
    }

    status = arrow::Table::FromRecordBatches(out_batches, &ret);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

	return ret;
}



BloomFilter* 
SelectExecutor::ConstructFilterNoFK(std::string dim_primary_key){

    arrow::Status status;
	arrow::compute::Datum* filter = GetBitFilter();
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> counts;
    arrow::compute::Datum* out = new arrow::compute::Datum();

    // Sum() fails because filter is a boolean array and not a numeric array...
    // Wait, we don't need to count how many times true appears in the filter to get the number of time we will insert
    // into the bloom filter; we can just take the length of the resulting output...!!!!!!!
    //status = arrow::compute::Sum(&function_context, *filter, out);
    //status = arrow::compute::Count(&function_context, *filter);
    /*
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    arrow::compute::ValueCounts(&function_context, *filter, &counts);
    arrow::compute::CastOptions cast_options;
    //arrow::compute::Cast(&function_context,*filter,arrow::int8(),cast_options, out);
    arrow::compute::Cast(&function_context,counts,counts->type(),cast_options, out);
    std::cout << out->make_array()->ToString() << std::endl;
    std::cout << counts->type() << std::endl;
    std::cout<<filter->length()<< " " <<counts->length() << std::endl;
     */
	int n = 500000;//out->length(); //@TODO: Fix this!!!!!!!!!

	BloomFilter* bf = new BloomFilter(n);

	/*

		add the bloom filter

	*/

    std::shared_ptr<arrow::RecordBatch> in_batch;


    auto* reader = new arrow::TableBatchReader(*dim_table);

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        status = arrow::compute::Filter(&function_context, in_batch->GetColumnByName(dim_primary_key), *filter, out);
        auto key_col = std::static_pointer_cast<arrow::Int64Array>(out->make_array());

        for (int i=0; i<key_col->length(); i++) {
            long long key = key_col->Value(i);
            bf -> Insert(key);
        }
    }

	return bf;
}
	





SelectExecutorComposite::SelectExecutorComposite(std::vector<SelectExecutor*> _children){

    dim_table = _children[0]->dim_table; // assuming that all child select exe have the same dim_table
	children = _children;
}


arrow::compute::Datum* 
SelectExecutorComposite::GetBitFilter(){
	int num_of_children = children.size();

	arrow::Status status;
	arrow::BooleanBuilder boolean_builder;
	std::shared_ptr<arrow::BooleanArray> boolean_array;

	status = boolean_builder.AppendValues(dim_table->num_rows(), false);
	EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
	status = boolean_builder.Finish(&boolean_array);
	EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto* filter = new arrow::compute::Datum(boolean_array);

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto* tmp = new arrow::compute::Datum(); /*initialize it to all 0*/

	for(int child_i = 0; child_i < num_of_children; child_i++){
		arrow::compute::Datum* child_filter = children[child_i] -> GetBitFilter();

		status = arrow::compute::Or(&function_context, *filter, *child_filter, tmp);
		EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

		filter = tmp;

		/* ret = ret || child_filter */
	}
	return filter;
}





SelectExecutorInt::SelectExecutorInt(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												long long _value, 
												arrow::compute::CompareOperator _op){
	dim_table = _dim_table;
	reader = new arrow::TableBatchReader(*_dim_table);
	select_field = _select_field;

	arrow::NumericScalar<arrow::Int64Type> myscalar(_value);
    value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);
	op = _op;
}



arrow::compute::Datum* 
SelectExecutorInt::GetBitFilter(){

	/* Get bit filter satisfying (integers op value) */

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(op);
    auto* filter = new arrow::compute::Datum();

    //@TODO: THIS IS BAD.
    std::shared_ptr<arrow::Table> tmp;
    status = dim_table->CombineChunks(arrow::default_memory_pool(), &tmp);
    std::shared_ptr<arrow::Array> col = tmp->GetColumnByName(select_field)->chunk(0); //we guarantee that there is only one chunk
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    // NOTE: Compare() has issues if when you try to compare a chunked array. I'm not sure why, but this may indicate
    // that comparisons should (or maybe even MUST) be done in batches.
    status = arrow::compute::Compare(&function_context, col, value, compare_options, filter);

    //@TODO: do a loop instead. You can pass individual filter batches to the parent node if we make the reader a member variable.
    // Wait, this is just a view over the table; this is fine, but the filter may be large!!!
    status = arrow::compute::Compare(&function_context, col, value, compare_options, filter);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    return filter;

}


arrow::compute::Datum*
SelectExecutorInt::GetNextBitFilterBatch(){

    /* Get bit filter satisfying (integers op value) */

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(op);
    auto* filter = new arrow::compute::Datum();

    status = reader->ReadNext(&in_batch);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), value, compare_options, filter);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    return filter;

}


SelectExecutorStr::SelectExecutorStr(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												std::string _value, 
												arrow::compute::CompareOperator _op){
	dim_table = _dim_table;
	select_field = _select_field;

	value = _value;
	op = _op;
}


arrow::compute::Datum* 
SelectExecutorStr::GetBitFilter(){

	/* Get bit filter satisfying (string op value) */
    arrow::Status status;

    arrow::BooleanBuilder boolean_builder;
    std::shared_ptr<arrow::BooleanArray> boolean_array;

    status = boolean_builder.Resize(dim_table->num_rows());
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    //@TODO: THIS IS BAD.
    std::shared_ptr<arrow::Table> tmp;
    status = dim_table->CombineChunks(arrow::default_memory_pool(), &tmp);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    auto col = std::static_pointer_cast<arrow::StringArray>(tmp->GetColumnByName(select_field)->chunk(0));//we guarantee that there is only one chunk


    for (int i=0; i<col->length(); i++) {
        status = boolean_builder.Append(EvaluatePredicate(col->GetString(i), value, op));
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    }

    status = boolean_builder.Finish(&boolean_array);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    auto* filter = new arrow::compute::Datum(boolean_array);

	return filter;
	
}



SelectExecutorBetween::SelectExecutorBetween(std::shared_ptr<arrow::Table> _dim_table, 
												std::string _select_field, 
												long long _lo_value, 
												long long _hi_value){
	dim_table = _dim_table;
	select_field = _select_field;

	arrow::NumericScalar<arrow::Int64Type> lo_scalar(_lo_value);
    lo_value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(lo_scalar);
	
	arrow::NumericScalar<arrow::Int64Type> hi_scalar(_hi_value);
    hi_value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(hi_scalar);
}


arrow::compute::Datum* 
SelectExecutorBetween::GetBitFilter(){
	arrow::compute::Datum* ret;
	/* Get bit filter satisfying (lo <= integers <= hi) */

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options_leq(arrow::compute::CompareOperator::LESS_EQUAL);
    arrow::compute::CompareOptions compare_options_geq(arrow::compute::CompareOperator::GREATER_EQUAL);
    auto* filter_leq = new arrow::compute::Datum();
    auto* filter_geq = new arrow::compute::Datum();
    auto* filter = new arrow::compute::Datum();

/*
    std::shared_ptr<arrow::Array> col = dim_table->GetColumnByName(select_field)->chunk(0);
    status = arrow::compute::Compare(&function_context, col, value, compare_options, filter);

    //@TODO: do a loop instead. You can pass individual filter batches to the parent node if we make the reader a member variable.
    // Wait, this is just a view over the table; this is fine, but the filter may be large!!!
    status = arrow::compute::Compare(&function_context, col, value, compare_options, filter);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
*/
    return filter;

	return ret;
}

SelectExecutorStrBetween::SelectExecutorStrBetween(std::shared_ptr<arrow::Table> _dim_table,
                                                   std::string _select_field,
                                                   std::string _lo_value,
                                                   std::string _hi_value) {
    dim_table = _dim_table;
    select_field = _select_field;

    lo_value = _lo_value;
    hi_value = _hi_value;
}

arrow::compute::Datum*
SelectExecutorStrBetween::GetBitFilter(){
    arrow::compute::Datum* ret;
    /* Get bit filter satisfying (lo <= strings <= hi) */
    return ret;
}



JoinExecutor::JoinExecutor(SelectExecutor* _s_exe, 
							std::string _dim_primary_key, 
							std::string _fact_foreign_key){
	fact_foreign_key = _fact_foreign_key;
	dim_primary_key = _dim_primary_key;
	select_exe = _s_exe;
}

std::shared_ptr<arrow::Table> JoinExecutor::join(std::shared_ptr<arrow::Table> fact_table){

	std::shared_ptr<arrow::Table> dim_table_selected = select_exe -> select();
	std::shared_ptr<arrow::Table> ret;
	ret = HashJoin(fact_table, fact_foreign_key, dim_table_selected, dim_primary_key);
	return ret;
}



BloomFilter* JoinExecutor::ConstructFilter(){
	BloomFilter* bf = select_exe -> ConstructFilterNoFK(dim_primary_key);
	bf -> SetForeignKey(fact_foreign_key);
	return bf;
}

