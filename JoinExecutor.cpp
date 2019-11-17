
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
    auto* out = new arrow::compute::Datum();

    status = arrow::compute::Sum(&function_context, *filter, out);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

	int n = 500000; //@TODO: Fix this!!!!!!!!!

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
/*
    arrow::BooleanBuilder boolean_builder;
    std::shared_ptr<arrow::BooleanArray> boolean_array;
    boolean_builder.Resize(dim_table->num_rows());

    auto* reader = new arrow::TableBatchReader(*dim_table);
*/
    //@TODO: do a loop instead
    // Wait, this is just a view over the table; this is fine, but the filter may be large!!!
    status = arrow::compute::Compare(&function_context, dim_table->GetColumnByName(select_field), value, compare_options, filter);
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
	arrow::compute::Datum* ret;
	/* Get bit filter satisfying (string op value) */
	return ret;
	
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
    /* Get bit filter satisfying (lo <= integers <= hi) */
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

