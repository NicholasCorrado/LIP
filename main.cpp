//
// Created by Nicholas Corrado on 10/29/19.
//
#include <iostream>
#include <map>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/stl.h>
#include <arrow/ipc/api.h>
#include "main.h"
#include "BuildFilter.h"

// If you successfully read the csv files but fail to write the arrow files, it might be because the arrow-output
// directory does not exist.
void write_to_file(std::string path, std::shared_ptr<arrow::Table> table) {

    std::shared_ptr<arrow::io::OutputStream> os;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    arrow::Status status;

    status = arrow::io::FileOutputStream::Open(path,&os);

    if(!status.ok()) {
        std::cout << "Error opening output stream." << std::endl;
    }

    writer = arrow::ipc::RecordBatchFileWriter::Open(&*os,table->schema()).ValueOrDie();
    status = writer->WriteTable(*table);

    if(!status.ok()) {
        std::cout << "Error writing table." << std::endl;
    }

    status = writer->Close();

    if(!status.ok()) {
        std::cout << "Error closing writer." << std::endl;
    }

}

/*
 * main SHOULD accept files as arguments. First argument is the path to the fact table. All following arguments are
 * the paths to dimension tables.
 */
int main_nick() {

    std::string file_path_customer  = "../benchmark/customer.tbl";
    std::string file_path_date      = "../benchmark/date.tbl";
    std::string file_path_lineorder = "../benchmark/lineorder.tbl";
    std::string file_path_part      = "../benchmark/part.tbl";
    std::string file_path_supplier  = "../benchmark/supplier.tbl";


    std::vector<std::string> customer_schema    = {"CUST KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE",
                                                   "MKT SEGMENT"};
    std::vector<std::string> date_schema        = {"DATE KEY", "DATE", "DAY OF WEEK", "MONTH", "YEAR", "YEAR MONTH NUM",
                                                   "YEAR MONTH", "DAY NUM IN WEEK", "DAY NUM IN MONTH", "MONTH NUM IN YEAR",
                                                   "DAY NUM IN YEAR",
                                                   "WEEK NUM IN YEAR", "SELLING SEASON", "LAST DAY IN WEEK FL",
                                                   "LAST DAT IN MONTH FL", "HOLIDAY FL", "WEEKDAY FL", };
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

    write_to_file("../arrow-output/customer.arrow", customer);
    write_to_file("../arrow-output/date.arrow", date);
    write_to_file("../arrow-output/lineorder.arrow", lineorder);
    write_to_file("../arrow-output/supplier.arrow", supplier);
    write_to_file("../arrow-output/part.arrow", part);

    std::shared_ptr<arrow::Table> result_table;
    //result_table = Select(customer, "MKT SEGMENT", "AUTOMOBILE", Operator::EQUAL);
    //PrintTable(result_table);

    result_table = Select(date, "YEAR", 1994, Operator::EQUAL);
    PrintTable(result_table);

    return 0;
}


int main_xiating(){
    std::string file_path_customer  = "../benchmark/customer.tbl";
    std::string file_path_date      = "../benchmark/date.tbl";
    std::string file_path_lineorder = "../benchmark/lineorder.tbl";
    std::string file_path_part      = "../benchmark/part.tbl";
    std::string file_path_supplier  = "../benchmark/supplier.tbl";


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

    
    BloomFilter* bf = BuildFilter(customer, "MKT SEGMENT", "AUTOMOBILE", Operator::EQUAL);
    
    std::string s1 = "asdf";
    std::cout << bf -> search(s1) << std::endl;
    std::string s2 = "AUTOMOBILE";
    std::cout << bf -> search(s2) << std::endl;
    return 0;
}


int main(){
    main_nick();

    //main_xiating();
    return 0;
}

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
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::shared_ptr<arrow::csv::TableReader> reader;
    status = arrow::csv::TableReader::Make(arrow::default_memory_pool(), infile, read_options, parse_options, convert_options, &reader);

    if(!status.ok()) {
        std::cout << "Error creating TableReader." << std::endl;
    }

    std::shared_ptr<arrow::Table> table;
    status = reader->Read(&table);

    EvaluateStatus(status);

    return table;
}
