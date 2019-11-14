#include <iostream>
#include <arrow/api.h>
#include "BuildFilter.h"

/* (String version)
	Given a table, a attribute field name and the selective criterion, return a Bloom
	Filter of all the attributes satisfyingg the criterion. 

	Input:
		table - the table to be selected from (e.g. dimension table)
		select_field - the field (attribute) column name
		value - the value to be compared
		op - the operator, e.g. =, <, >

	Return
		A Bloomfilter bf of all the attributes passing the selection
*/
BloomFilter* 
BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, std::shared_ptr<arrow::Scalar> value, arrow::compute::CompareOperator op, std::string key_field, std::string foreign_key) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    
    auto* reader = new arrow::TableBatchReader(*table);

    BloomFilter* bf = new BloomFilter();

    bf -> SetForeignKey(foreign_key);

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(op);
    auto* filter = new arrow::compute::Datum();

    // The Status outcome object returned by ReadNext() does NOT return an error when you are already at the final
    // record batch; it sets the output batch to nullptr and returns Status::OK(). Hence, we must also check that the
    // output batch is not nullptr.
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        auto* out = new arrow::compute::Datum();

        status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), value, compare_options, filter); // bool array
        status = arrow::compute::Filter(&function_context, in_batch->GetColumnByName(key_field), *filter, out);

        auto key_col = std::static_pointer_cast<arrow::Int64Array>(out->make_array());

        for (int i=0; i<key_col->length(); i++) {
                bf -> Insert(key_col->Value(i));
        }
    }
    return bf;
}


/* (Int64 version), one sided compare

    Given a table, a attribute field name and the selective criterion, return a Bloom
    Filter of all the attributes satisfyingg the criterion. 

    Input:
        table - the table to be selected from (e.g. dimension table)
        select_field - the field (attribute) column name
        value - the value to be compared
        op - the operator, e.g. =, <, >

    Return
        A Bloomfilter bf of all the attributes passing the selection
*/
/*
BloomFilter* 
BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, long long value, Operator op, std::string key_field, std::string foreign_key) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    
    auto* reader = new arrow::TableBatchReader(*table);

    BloomFilter* bf = new BloomFilter();
    bf -> SetForeignKey(foreign_key);
    // The Status outcome object returned by ReadNext() does NOT return an error when you are already at the final
    // record batch; it sets the output batch to nullptr and returns Status::OK(). Hence, we must also check that the
    // output batch is not nullptr.
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        // Arrays must be downcasted to a concrete array type before we can access the array items. Note that the
        // method used to access item i of an array depend on the data type, e.g. GetString(i) for StringArray vs
        // Value(i) for Int64Array (and any other int array).
        auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(select_field));
        auto key_col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(key_field));
        
        for (int i=0; i<col->length(); i++) {
            long long attr = col->Value(i);
            if ( EvaluatePredicate(attr, value, op) ) {
                bf -> Insert(key_col -> Value(i));
            }
        }
    }
    return bf;
}

*/

/* (Int64 version), supporting between

    Given a table, a attribute field name and the selective criterion, return a Bloom
    Filter of all the attributes satisfyingg the criterion. 

    Input:
        table - the table to be selected from (e.g. dimension table)
        select_field - the field (attribute) column name
        lo_value - the lo value to be compared
        hi_value - the hi value to be compared

    Return
        A Bloomfilter bf of all the attributes passing the selection
*/


BloomFilter*
BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, std::shared_ptr<arrow::Scalar> lo_value,
        std::shared_ptr<arrow::Scalar> hi_value, std::string key_field, std::string foreign_key) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    
    auto* reader = new arrow::TableBatchReader(*table);

    BloomFilter* bf = new BloomFilter();
    bf -> SetForeignKey(foreign_key);

    // Instantiate things needed for a call to Compare()
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options_leq(arrow::compute::CompareOperator::LESS_EQUAL);
    arrow::compute::CompareOptions compare_options_geq(arrow::compute::CompareOperator::GREATER_EQUAL);
    auto* filter_leq = new arrow::compute::Datum();
    auto* filter_geq = new arrow::compute::Datum();
    auto* filter = new arrow::compute::Datum();

    // The Status outcome object returned by ReadNext() does NOT return an error when you are already at the final
    // record batch; it sets the output batch to nullptr and returns Status::OK(). Hence, we must also check that the
    // output batch is not nullptr.
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {

        auto* out = new arrow::compute::Datum();

        status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), lo_value, compare_options_geq, filter_geq); // bool array
        status = arrow::compute::Compare(&function_context, in_batch->GetColumnByName(select_field), hi_value, compare_options_leq, filter_leq); // bool array
        status = arrow::compute::And(&function_context,*filter_leq, *filter_geq, filter);

        status = arrow::compute::Filter(&function_context, in_batch->GetColumnByName(key_field), *filter, out);

        auto key_col = std::static_pointer_cast<arrow::Int64Array>(out->make_array());

        for (int i=0; i<key_col->length(); i++) {
            bf -> Insert(key_col->Value(i));
        }
    }
    return bf;
}

