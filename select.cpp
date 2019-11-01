//
// Created by Nicholas Corrado on 11/1/19.
//

#include <iostream>
#include <arrow/api.h>


#include "select.h"

/*
 * Design decision: select() should accept a table, not an arrow file. The table must be constructed from the file
 * before select() is to be called.
 */
void select(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value) {

    std::shared_ptr<arrow::Table> result;
    // Our template query retains all columns
    std::shared_ptr<arrow::Schema> result_schema = table->schema();

    arrow::Status status;
    arrow::TableBatchReader r;
}

void select_old(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value) {

    std::shared_ptr<arrow::Table> result;
    std::shared_ptr<arrow::Schema> result_schema = table->schema();

    arrow::Status status;

    std::unique_ptr<arrow::ArrayBuilder> b;
    status = arrow::MakeBuilder(arrow::default_memory_pool(), result_schema->field(0)->type(), &b);

    std::cout<< b->type()->ToString() <<std::endl;
    std::cout<<result_schema->field(0)->type()->id()<<std::endl;
    //std::cout<<arrow::Type::type << std::endl;

    std::vector<arrow::ArrayBuilder*> builders;

    for (int i=0; i<result_schema->num_fields(); i++) {
        int type = result_schema->field(i)->type()->id();

        arrow::StringBuilder* b_string = new arrow::StringBuilder;
        arrow::Int64Builder* b_int = new arrow::Int64Builder;

        switch (type) {
            case arrow::Type::type::STRING: {
                builders.push_back(b_string);
                break;
                case arrow::Type::type::INT64:
                    builders.push_back(b_int);
                break;
            }

        }

    }

    for (int i=0; i<result_schema->num_fields(); i++) {
        std::cout << builders.at(i)->type()->ToString() << std::endl;
    }

    //std::tuple t = std::make_tuple();

    std::shared_ptr<arrow::Array> chunk = table->GetColumnByName(select_field)->chunk(0); // only one chunk

    std::vector<int> rows;

    for (int i=0; i<chunk->length(); i++) {
        std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(chunk);
        std::string s = array->GetString(i);
        if (s == "AUTOMOBILE") {
            rows.push_back(i);
        }
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (int i=0; i<result_schema->num_fields(); i++) {

        int type = result_schema->field(i)->type()->id();
        std::shared_ptr<arrow::Array> a;


        switch (type) {
            case arrow::Type::type::STRING: {
                arrow::StringBuilder builder;
                for (int row : rows) {
                    std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(table->column(i)->chunk(0));
                    builder.Append(array->GetString(row));
                }
                builder.Finish(&a);
                break;
            }
            case arrow::Type::type::INT64: {
                arrow::Int64Builder builder;
                for (int row2 : rows) {

                    std::shared_ptr<arrow::Int64Array> array = std::static_pointer_cast<arrow::Int64Array>(table->column(i)->chunk(0));
                    builder.Append(array->Value(row2));
                }
                builder.Finish(&a);
                break;
            }
        }

        arrays.push_back(std::move(a));
    }
    std::shared_ptr<arrow::Table> t = arrow::Table::Make(result_schema,arrays);
}