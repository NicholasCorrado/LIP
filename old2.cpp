//
// Created by Nicholas Corrado on 10/29/19.
//
#include <iostream>
#include <map>

#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/stl.h>
#include <arrow/ipc/api.h>
#include "main.h"

#include <tuple>

void write_to_file(std::string path, std::shared_ptr<arrow::Table> table) {

    std::shared_ptr<arrow::io::OutputStream> os;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;

    arrow::io::FileOutputStream::Open(path,&os);
    writer = arrow::ipc::RecordBatchFileWriter::Open(&*os,table->schema()).ValueOrDie();
    writer->WriteTable(*table);
    writer->Close();
}

int main() {

    std::string file_path_customer  = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/customer.tbl";
    std::string file_path_date      = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/date.tbl";
    std::string file_path_lineorder = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/lineorder.tbl";
    std::string file_path_part      = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/part.tbl";
    std::string file_path_supplier  = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/supplier.tbl";


    std::vector<std::string> customer_schema    = {"CUST KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE",
                                                   "MKT SEGMENT"};
    std::vector<std::string> date_schema        = {"DATE KEY", "DATE", "DAY OF WEEK", "MONTH", "YEAR", "YEAR MONTH NUM",
                                                   "YEAR MONTH", "DAY NUM IN WEEK", "DAY NUM IN MONTH", "MONTH NUM IN YEAR",
                                                   "WEEK NUM IN YEAR", "SELLING SEASON", "LAST DAY IN WEEK FL",
                                                   "LAST DAT IN MONTH FL", "HOLIDAY FL", "WEEKDAY FL", "DAY NUM YEAR"};
    std::vector<std::string> lineorder_schema   = {"ORDER KEY", "LINE NUMBER", "CUST KEY", "PART KEY", "SUPP KEY",
                                                   "ORDER DATE", "ORD PRIORITY", "SHIP PRIORITY", "QUANTITY",
                                                   "EXTENDED PRICE", "ORD TOTAL PRICE", "DISCOUNT", "REVENUE",
                                                   "SUPPLY COST", "TAX", "COMMIT DATE", "SHIP MODE"};
    std::vector<std::string> part_schema        = {"PART KEY", "NAME", "MFGR", "CATEGORY", "BRAND", "COLOR", "TYPE", "SIZE", "CONTAINER"};
    std::vector<std::string> supplier_schema    = {"SUPP KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE"};

    std::shared_ptr<arrow::Table> customer;
    std::shared_ptr<arrow::Table> date;
    std::shared_ptr<arrow::Table> lineorder;
    std::shared_ptr<arrow::Table> part;
    std::shared_ptr<arrow::Table> supplier;

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    customer    = build_table(file_path_customer,  pool, customer_schema);
    date        = build_table(file_path_date,      pool, date_schema);
    lineorder   = build_table(file_path_lineorder, pool, lineorder_schema);
    part        = build_table(file_path_part,      pool, part_schema);
    supplier    = build_table(file_path_supplier,  pool, supplier_schema);

    std::cout<<"customer->num_rows() = " << customer->num_rows() << std::endl;
    std::cout<<"date->num_rows() = " << date->num_rows() << std::endl;
    std::cout<<"lineorder->num_rows() = " << lineorder->num_rows() << std::endl;
    std::cout<<"part->num_rows() = " << part->num_rows() << std::endl;
    std::cout<<"supplier->num_rows() = " << supplier->num_rows() << std::endl;

    write_to_file("/Users/corrado/cs764project/customer.arrow", customer);
    write_to_file("/Users/corrado/cs764project/date.arrow", date);
    write_to_file("/Users/corrado/cs764project/lineorder.arrow", lineorder);
    write_to_file("/Users/corrado/cs764project/supplier.arrow", supplier);
    write_to_file("/Users/corrado/cs764project/part.arrow", part);

    // build hash table on the last column of customer
    std::shared_ptr<arrow::ChunkedArray> column = customer->column(7);
    arrow::ArrayVector v = column->chunks();
    std::unordered_multimap<std::string, int> hash_table;
    std::hash<std::string> h;

    std::cout << arrow::int8()->layout().bit_widths.at(1) << std::endl;
    // select query should be easy, since our template queries will--wait, why not just accept a schema as a parameter??
    // or rather, accept the array builders as parameters.
    // We know all the Di pks will be Int64, so why not just make an array of Int64Builders?? and then you can has a
    // separate builder for the "payload," whatever that is.

    std::shared_ptr<arrow::Table> t;

    arrow::Int64Builder b0;
    arrow::StringBuilder b1;
    arrow::StringBuilder b2;
    arrow::StringBuilder b3;
    arrow::StringBuilder b4;
    arrow::StringBuilder b5;
    arrow::StringBuilder b6;
    arrow::StringBuilder b7;

    std::shared_ptr<arrow::Array> chunk = column->chunk(0); // only one chunk

    select(customer, "MKT SEGMENT", "AUTOMOBILE");
    /*
    for (int i=0; i<chunk->length(); i++) {
        std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(chunk);
        std::string s = array->GetString(i);
        if (s == "AUTOMOBILE") {
            std::shared_ptr<arrow::Array> chunk1 = customer->column(0)->chunk(0); // one chunk
            std::shared_ptr<arrow::Array> chunk2 = customer->column(1)->chunk(0);

            std::shared_ptr<arrow::Int64Array> array1 = std::static_pointer_cast<arrow::Int64Array>(chunk1);
            std::shared_ptr<arrow::StringArray> array2 = std::static_pointer_cast<arrow::StringArray>(chunk2);

            std::cout << array1->GetView(i) << " " << array2->GetView(i) << std::endl;

        }
    }
    */
    /*
    for (int i=0; i<column->num_chunks(); i++) {
        std::shared_ptr<arrow::Array> chunk = column->chunk(i);
        std::cout<< "chunk size = "<< chunk->length() << std::endl;
        std::cout<< "offset = "<< column->type()->ToString() << std::endl;
        std::cout<< "size = "<< chunk->data()<< std::endl;



        for (int j = 0; j < chunk->length(); j++) {
            std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(chunk);
            std::string s = array->GetString(j);

            if (s == "AUTOMOBILE") {


                std::shared_ptr<arrow::Int64Array> array_1 = std::static_pointer_cast<arrow::Int64Array>(chunk);
                int val_1 = array_1->Value(j);
                b1.Append(val_1);

            }
            //hash_table.insert({s, j});
        }

    }
    */
    // wait, what we're you doing?? don't join??? just do a select???????????
    /*
     *     int row_F_i = 0;
    int chunk_F_i = 0;
     *
    for (int i=0; i<column->num_chunks(); i++) {
        std::shared_ptr<arrow::Array> chunk = column->chunk(i);
        std::cout<<"num chunks = " << chunk->length() << std::endl;
        for (int j = 0; j < chunk->length(); j++) {

            row_F_i++;

            std::shared_ptr<arrow::Array> chunk_F = lineorder->column(0)->chunk(chunk_F_i);

            if (row_F_i >= chunk_F->length()) {
                chunk_F = lineorder->column(0)->chunk(chunk_F_i++);
                row_F_i = 0;
            }

            // i need to make sure I'm looking at the correct chunk what I add rows to bF...
            std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(chunk);
            std::string s = array->GetString(j);
            if (s == "AUTOMOBILE") {
                std::shared_ptr<arrow::Int64Array> array_F = std::static_pointer_cast<arrow::Int64Array>(chunk_F);
                int val = array_F->Value(row_F_i);
                bF.Append(val);

                std::shared_ptr<arrow::Int64Array> array_1 = std::static_pointer_cast<arrow::Int64Array>(chunk);
                int val_1 = array_1->Value(j);
                b1.Append(val_1);

                std::cout << val << " " << val_1 << std::endl;
            }
            //hash_table.insert({s, j});
        }
    }
    std::cout << b1.length() << std::endl;
    std::cout << bF.length() << std::endl;
     */
    //std::cout << hash_table.size() << std::endl;
    //std::cout << hash_table. << std::endl;
    //std::cout << hash_table.begin(0) << std::endl;

    for (auto& x: hash_table) {
        std::cout << "Element [" << x.first << ":" << x.second << "]";
        std::cout << " is in bucket #" << hash_table.bucket(x.first) << std::endl;
    }




    /*
    for (int i=0; i<customer->schema()->num_fields(); i++) {
        std::cout << customer->schema()->field(i)->type()->ToString() << std::endl;
        //std::cout << date->schema()->field(i)->type()->ToString() << std::endl;
        //std::cout << lineorder->schema()->field(i)->type()->ToString() << std::endl;
        //std::cout << part->schema()->field(i)->type()->ToString() << std::endl;
        //std::cout << supplier->schema()->field(i)->type()->ToString() << std::endl;
    }

    //arrow::ArrayBuilder b = arrow::ArrayBuilder(pool);
    //b.type() = customer->field(0)->type();
    //std::cout<< "type = " << b.type() << std::endl;
    */
}



// Do I need to pass in a memory pool as a parameter?
std::shared_ptr<arrow::Table>
build_table(const std::string& file_path, arrow::MemoryPool *pool, std::vector<std::string> &schema) {

    arrow::Status status;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    status = arrow::io::ReadableFile::Open(file_path, pool, &infile);

    if(!status.ok()) {
        std::cout << "Error reading file." << std::endl;
    }

    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.column_names = schema;
    std::cout << "BLOCK SIZE = " << read_options.block_size << std::endl;
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::shared_ptr<arrow::csv::TableReader> reader;
    status = arrow::csv::TableReader::Make(arrow::default_memory_pool(), infile, read_options, parse_options, convert_options, &reader);

    if(!status.ok()) {
        std::cout << "Error creating TableReader." << std::endl;
    }

    std::shared_ptr<arrow::Table> tmp_table;
    std::shared_ptr<arrow::Table> table;
    status = reader->Read(&tmp_table);
    //arrow::Table::CombineChunks(pool, &table);

    if(!status.ok()) {
        std::cout << "Error reading csv into table." << std::endl;
    }

    tmp_table->CombineChunks(pool, &table);

    return table;

    /*
    std::shared_ptr<arrow::Table> tmp_table;
    std::shared_ptr<arrow::Table> table;
    status = reader->Read(&tmp_table);

    if(!status.ok()) {
    std::cout << "Error reading csv into table." << std::endl;
    }

    // THIS IS PROBABLY UNNECESSARY NOW SINCE WE'LL BE READING IN ARROW FILES IN BATCHES
    // Apprently writing to a file only works if chunks are combined?? No, wait, if I comment out this section,
    // I'm returning a nullptr (table). I should be returning tmp_table.
    status = tmp_table->CombineChunks(pool, &table);

    if(!status.ok()) {
    std::cout << "Error combining chunks" << std::endl;
    }

    return tmp_table;
    */
}

//you can just overload the function with string and int parameters
// Build the table one column at a time??
// When I find a row I need to keep, how should I store it in the result table? You can't really build the table row by row...
// The issue here is that I will need to create a variable number of array builders to construct the result array.
// Ah, yes! We can build a table from a vector of arrays. Loop over the fields we want, and add an array builder to the
// vector for each one!

// select field can also be a vector!
void select(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value) {

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
                // printing an enum return its index...
                //std::cout<< type << std::endl;
        }

    }

    for (int i=0; i<result_schema->num_fields(); i++) {
        std::cout << builders.at(i)->type()->ToString() << std::endl;
    }

    //arrow::Type::type::INT8;
    //arrow::NumericBuilder<> y;

    //std::tuple t = std::make_tuple();

    std::shared_ptr<arrow::Array> chunk = table->GetColumnByName(select_field)->chunk(0); // only one chunk

    std::vector<int> rows;

    for (int i=0; i<chunk->length(); i++) {
        std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(chunk);
        std::string s = array->GetString(i);
        if (s == "AUTOMOBILE") {
            rows.push_back(i);
            //std::shared_ptr<arrow::Array> chunk1 = table->column(0)->chunk(0); // one chunk
            //std::shared_ptr<arrow::Array> chunk2 = table->column(1)->chunk(0);

            //std::shared_ptr<arrow::Int64Array> array1 = std::static_pointer_cast<arrow::Int64Array>(chunk1);
            //std::shared_ptr<arrow::StringArray> array2 = std::static_pointer_cast<arrow::StringArray>(chunk2);
            /*
                    for (int i=0; i<result_schema->num_fields(); i++) {
                        static_cast<arrow::>.at(i)
                    }
                    */
            //std::cout << array1->GetView(i) << " " << array2->GetView(i) << std::endl;

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

void select(std::shared_ptr<arrow::Table> table, std::vector<std::string> fields, std::vector<arrow::DataType> values) {

    std::vector<std::string> column_names = table->ColumnNames();
    int col_i;
    //std::vector<std::shared_ptr<arrow::DataType>> test;
    //test.push_back(arrow::int8);
    //double f = arrow::int8();

    for (int i=0; i<fields.size(); i++) {
        std::string field = fields.at(i);
        //arrow::Type::DOUBLE

        //arrow::DataType:: value = values.at(i);
    }
    /*
    for (std::string field : fields) {
        table->GetColumnByName(field)->
    }
    */
    // need to check that the specified columns actually exist.
    /*
    std::vector<std::string>::iterator iter = std::find(column_names.begin(), column_names.end(), field);

    if (iter == column_names.end()) {
        std::cout << "Column not found in table. Exiting function now." << std::endl;
        return;
    }
    col_i = std::distance(iter, column_names.end());
    */



}

std::shared_ptr<arrow::Array> get_row(std::shared_ptr<arrow::Table> table, uint row_i) {

    return nullptr;

}

