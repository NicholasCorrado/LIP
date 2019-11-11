#include <iostream>
#include <arrow/api.h>
#include "BuildFilter.h"


// Currently only supports queries on columns with string data.
/*
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
BuildFilter(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, Operator op) {

    arrow::Status status;
    std::shared_ptr<arrow::RecordBatch> in_batch;
    auto* reader = new arrow::TableBatchReader(*table);

    // The Status outcome object returned by ReadNext() does NOT return an error when you are already at the final
    // record batch; it sets the output batch to nullptr and returns Status::OK(). Hence, we must also check that the
    // output batch is not nullptr.

    std::vector<std::string> strings;

    BloomFilter* bf = new BloomFilter();

    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
        int type = table->schema()->GetFieldByName(select_field)->type()->id();

        // Arrays must be downcasted to a concrete array type before we can access the array items. Note that the
        // method used to access item i of an array depend on the data type, e.g. GetString(i) for StringArray vs
        // Value(i) for Int64Array (and any other int array).
        switch (type) {
            case arrow::Type::type::STRING: {
                auto col = std::static_pointer_cast<arrow::StringArray>(in_batch->GetColumnByName(select_field));

                for (int i=0; i<col->length(); i++) {
                	std::string attr = col->GetString(i);
                    if ( EvaluatePredicate(attr, value, op) ) {
                        bf -> insert(attr);
                    }
                }
                break;
            }
                /*
                case arrow::Type::type::INT64: {
                    auto col = std::static_pointer_cast<arrow::Int64Array>(in_batch->GetColumnByName(select_field));

                    for (int i=0; i<col->length(); i++) {
                        if ( EvaluatePredicate(col->Value(i), value, op) )) {
                            AddRowToRecordBatch(i, in_batch, out_batch_builder);
                        }
                    }
                    break;
                }
                */
        }
    }
    return bf;
}

