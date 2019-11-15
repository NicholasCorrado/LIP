//
// Created by Nicholas Corrado on 11/15/19.
//


#include "Queries.h"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/stl.h>

int main_nick() {

    std::string file_path_customer = "benchmark/customer.tbl";
    std::string file_path_date = "benchmark/date.tbl";
    std::string file_path_lineorder = "benchmark/lineorder.tbl";
    std::string file_path_part = "benchmark/part.tbl";
    std::string file_path_supplier = "benchmark/supplier.tbl";


    std::vector <std::string> customer_schema = {"CUST KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE",
                                                 "MKT SEGMENT"};
    std::vector <std::string> date_schema = {"DATE KEY", "DATE", "DAY OF WEEK", "MONTH", "YEAR", "YEAR MONTH NUM",
                                             "YEAR MONTH", "DAY NUM IN WEEK", "DAY NUM IN MONTH", "MONTH NUM IN YEAR",
                                             "DAY NUM IN YEAR",
                                             "WEEK NUM IN YEAR", "SELLING SEASON", "LAST DAY IN WEEK FL",
                                             "LAST DAT IN MONTH FL", "HOLIDAY FL", "WEEKDAY FL",};
    std::vector <std::string> lineorder_schema = {"ORDER KEY", "LINE NUMBER", "CUST KEY", "PART KEY", "SUPP KEY",
                                                  "ORDER DATE", "ORD PRIORITY", "SHIP PRIORITY", "QUANTITY",
                                                  "EXTENDED PRICE", "ORD TOTAL PRICE", "DISCOUNT", "REVENUE",
                                                  "SUPPLY COST", "TAX", "COMMIT DATE", "SHIP MODE"};
    std::vector <std::string> part_schema = {"PART KEY", "NAME", "MFGR", "CATEGORY", "BRAND", "COLOR", "TYPE", "SIZE",
                                             "CONTAINER"};
    std::vector <std::string> supplier_schema = {"SUPP KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE"};

    std::shared_ptr <arrow::Table> customer;
    std::shared_ptr <arrow::Table> date;
    std::shared_ptr <arrow::Table> lineorder;
    std::shared_ptr <arrow::Table> part;
    std::shared_ptr <arrow::Table> supplier;

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    customer = build_table(file_path_customer, pool, customer_schema);
    date = build_table(file_path_date, pool, date_schema);
    lineorder = build_table(file_path_lineorder, pool, lineorder_schema);
    part = build_table(file_path_part, pool, part_schema);
    supplier = build_table(file_path_supplier, pool, supplier_schema);

    std::cout << "customer->num_rows() = " << customer->num_rows() << std::endl;
    std::cout << "date->num_rows() = " << date->num_rows() << std::endl;
    std::cout << "lineorder->num_rows() = " << lineorder->num_rows() << std::endl;
    std::cout << "part->num_rows() = " << part->num_rows() << std::endl;
    std::cout << "supplier->num_rows() = " << supplier->num_rows() << std::endl;

    write_to_file("arrow-output/customer.arrow", customer);
    write_to_file("arrow-output/date.arrow", date);
    write_to_file("arrow-output/lineorder.arrow", lineorder);
    write_to_file("arrow-output/supplier.arrow", supplier);
    write_to_file("arrow-output/part.arrow", part);

    std::shared_ptr <arrow::Table> result_table;
    //result_table = Select(customer, "MKT SEGMENT", "AUTOMOBILE", Operator::EQUAL);
    //PrintTable(result_table);

    //result_table = Select(date, "YEAR", 1994, Operator::EQUAL);
    // arrow::NumericScalar<arrow::Int64Type> myscalar(1992);
    // auto value = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);
    // arrow::NumericScalar<arrow::Int64Type> myscalar2(1994);
    // auto value2 = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);
    // arrow::NumericScalar<arrow::Int64Type> myscalar3(10);
    // auto size = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(myscalar);

    //result_table = Select(date, "YEAR", value, arrow::compute::CompareOperator::EQUAL);
    //PrintTable(result_table);


    //BloomFilter* bf_part = BuildFilter(part, "SIZE", size,arrow::compute::CompareOperator::GREATER_EQUAL,"PART KEY", "PART KEY");
    //BloomFilter* bf_date = BuildFilter(date, "YEAR", value, value2, "DATE KEY", "ORDER DATE");

    // BloomFilter* bf_part = BuildFilter(part, "SIZE", size,arrow::compute::CompareOperator::GREATER_EQUAL,"PART KEY");
    // BloomFilter* bf_date = BuildFilter(date, "YEAR", value, value2, "DATE KEY");
    // std::cout<<"Join lineorder and customer on CUST KEY"<<std::endl;
    // std::shared_ptr<arrow::Table> ret = HashJoin(lineorder, "CUST KEY", customer, "CUST KEY");
    // //PrintTable(ret);
    // std::cout<<"ret->num_rows() = " << ret->num_rows()<<std::endl;

    // //result_table = Select(date, "YEAR", value, arrow::; compute::CompareOperator::EQUAL);
    // arrow::NumericScalar<arrow::Int64Type> one(1);
    // std::shared_ptr<arrow::Scalar> test;

    // auto custkey = std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(one);
    // result_table = Select(customer, "CUST KEY", custkey, arrow::compute::CompareOperator::EQUAL);
    // std::cout<<"ret->num_rows() = " << ret->num_rows()<<std::endl;
    // std::cout<<"Join lineorder and customer on CUST KEY where customer.CUST KEY = 1"<<std::endl;
    //ret = HashJoin(lineorder, "CUST KEY", result_table, "CUST KEY");
    //PrintTable(ret);
    //std::cout<<"ret->num_rows() = " << ret->num_rows()<<std::endl;
    //PrintTable(ret);

    SelectExecutor *date_s_exe = new SelectExecutorInt(date, "YEAR", 1995, arrow::compute::CompareOperator::EQUAL);
    //SelectExecutor *customer_s_exe = new SelectExecutorInt(customer, "CUST KEY", 5,arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *customer_s_exe = new SelectExecutorStr(customer, "REGION", "AMERICA",arrow::compute::CompareOperator::EQUAL);
    JoinExecutor *j_exe1 = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *j_exe2 = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");

    //std::vector<JoinExecutor*> tree = {j_exe1, j_exe2};
    std::vector < JoinExecutor * > tree = {j_exe1, j_exe2};
    std::cout << "Join lineorder and customer and date " << std::endl;
    std::shared_ptr <arrow::Table> t1 = EvaluateJoinTreeLIP(lineorder, tree);
    //PrintTable(t1);
    std::cout << t1->num_rows() << std::endl;
    std::shared_ptr <arrow::Table> t2 = EvaluateJoinTree(lineorder, tree);
    //PrintTable(t2);
    std::cout << t2->num_rows() << std::endl;

    //SelectExecutor* customer_s_exe = new SelectExecutorCompare(customer, "CUST KEY", 1, arrow::compute::CompareOperator::EQUAL);
    //JoinExecutor* j_exe = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");


    return 0;
}



int main_xiating(){
    std::string file_path_customer  = "./benchmark/customer.tbl";
    std::string file_path_date      = "./benchmark/date.tbl";
    std::string file_path_lineorder = "./benchmark/lineorder.tbl";
    std::string file_path_part      = "./benchmark/part.tbl";
    std::string file_path_supplier  = "./benchmark/supplier.tbl";


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


    // std::shared_ptr<arrow::Table> result_table = s_exe -> select();

    // std::cout << result_table->num_rows() << std::endl;


    SelectExecutor* date_s_exe = new SelectExecutorInt(date, "YEAR", 1992, arrow::compute::CompareOperator::EQUAL);
    JoinExecutor* j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");

    BloomFilter* bf = j_exe -> ConstructFilter();


    std::cout << bf -> Search(19920101) << std::endl;
    std::cout << bf -> Search(19930101) << std::endl;


    // ALWAYS LEFT JOIN FACT TABLE AND RIGHT JOIN CUSTOMER TABLE
    // std::shared_ptr<arrow::Table> result_table;

    // result_table = HashJoin(lineorder, "CUST KEY", Select(customer, "REGION", "AMERICA", Operator::EQUAL), "CUST KEY");

    // std::cout << result_table -> num_rows() << std::endl;



    // BLOOM FILTER IN THE FOLLOWING


    // BloomFilter* bf_customer = BuildFilter(customer, "REGION", "AMERICA", Operator::EQUAL, "CUST KEY", "CUST KEY");
    // BloomFilter* bf_date = BuildFilter(date, "YEAR", 1992, 1994, "DATE KEY", "ORDER DATE");
    // BloomFilter* bf_part = BuildFilter(part, "COLOR", "dark", Operator::EQUAL, "PART KEY", "PART KEY");
    // BloomFilter* bf_supplier = BuildFilter(supplier, "REGION", "AMERICA", Operator::EQUAL, "SUPP KEY", "SUPP KEY");


    // BloomFilter* bf[4] = {bf_customer, bf_date, bf_part, bf_supplier};
    // int size = 4;

    // ITERATE THROUGH THE FACT TABLE
    // arrow::Status status;
    // std::shared_ptr<arrow::RecordBatch> in_batch;

    // auto* reader = new arrow::TableBatchReader(*lineorder);


    // result_table = Select(date, "YEAR", 1994, Operator::EQUAL);
    //PrintTable(result_table);
    // std::cout << result_table -> num_rows() << std::endl;
    // int block_size = 64;

    // while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr) {
    //     std::sort(bf, bf+size, BloomFilterCompare);
    //     std::cout << bf[i_filter] -> GetFilterRate() << " ";

    //     for (int i=0; i<col->length(); i++) {
    //         long long attr = col->Value(i);

    //         std::cout << attr << std::endl;

    //     }
    // }



    return 0;
}

