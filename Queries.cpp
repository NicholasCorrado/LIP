//
// Created by Nicholas Corrado on 11/15/19.
//



#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/stl.h>
#include <chrono>
#include "Queries.h"
#include "select.h"

/*

All queries status:

    1.1 - need select on fact table, now just select
    1.2 - need select on fact table, now just select
    1.3 - need select on fact table, 
            need evaluating and on one dim table

    2.1 - need retrieve all dim table
    2.2 - need between on string, retrieve all dim table
    2.3 - need retrieve all dim table

    3.1 - done
    3.2 - done
    3.3 - need evaluating or on one dim table
    3.4 - need evaluating or on one dim table

    4.1 - need evaluating or on one dim table
    4.2 - need evaluating or on one dim table
    4.3 - need evaluating or on one dim table


*/




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
    SelectExecutor *customer_s_exe = new SelectExecutorInt(customer, "CUST KEY", 5,arrow::compute::CompareOperator::EQUAL);
    //SelectExecutor *customer_s_exe = new SelectExecutorStr(customer, "REGION", "AMERICA",arrow::compute::CompareOperator::EQUAL);
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


    RunAllQueries(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);

    return 0;
}

void RunAllQueries(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    Query1_1(customer, date, lineorder, part, supplier, alg_flag);
    /*
    Query1_2(customer, date, lineorder, part, supplier, alg_flag);
    Query1_3(customer, date, lineorder, part, supplier, alg_flag);


    Query2_1(customer, date, lineorder, part, supplier, alg_flag);
    Query2_2(customer, date, lineorder, part, supplier, alg_flag);
    Query2_3(customer, date, lineorder, part, supplier, alg_flag);


    Query3_1(customer, date, lineorder, part, supplier, alg_flag);
    Query3_2(customer, date, lineorder, part, supplier, alg_flag);
    Query3_3(customer, date, lineorder, part, supplier, alg_flag);
    Query3_4(customer, date, lineorder, part, supplier, alg_flag);


    Query4_1(customer, date, lineorder, part, supplier, alg_flag);
    Query4_2(customer, date, lineorder, part, supplier, alg_flag);
    Query4_3(customer, date, lineorder, part, supplier, alg_flag);
     */
}


void AlgorithmSwitcher(std::shared_ptr <arrow::Table> lineorder, std::vector<JoinExecutor*> tree, int alg_flag){
    switch (alg_flag){
        case ALG::HASH_JOIN:
            EvaluateJoinTree(lineorder, tree);
            break;
        case ALG::LIP_STANDARD:
            EvaluateJoinTreeLIP(lineorder, tree);
            break;
        default:
            std::cout << "Unknown algorithm" << std::endl;
            break;
    }
}



// WE NEED SELECT ON FACT TABLE HERE
void Query1_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 1.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    arrow::NumericScalar<arrow::Int64Type> one(1);
    arrow::NumericScalar<arrow::Int64Type> three(3);
    arrow::NumericScalar<arrow::Int64Type> twentyfive(25);
    
    lineorder = SelectBetween(lineorder, "DISCOUNT", 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(one), 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(three));
    lineorder = Select(lineorder, "QUANTITY", 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(twentyfive), arrow::compute::CompareOperator::LESS);

    SelectExecutor *date_s_exe      = new SelectExecutorInt( // here should be a pure join
                                            date, "YEAR", 1993, arrow::compute::CompareOperator::EQUAL);
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    

    std::vector <JoinExecutor*> tree = {date_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);
    



    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}



// WE NEED SELECT ON FACT TABLE HERE
void Query1_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 1.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    
    // produce the join tree by constructing the SelectExecutor and JoinExecutor:
    
    arrow::NumericScalar<arrow::Int64Type> four(4);
    arrow::NumericScalar<arrow::Int64Type> six(6);
    arrow::NumericScalar<arrow::Int64Type> twentysix(26);
    arrow::NumericScalar<arrow::Int64Type> thirtyfive(35);
    
    lineorder = SelectBetween(lineorder, "DISCOUNT", 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(four), 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(six));
    lineorder = SelectBetween(lineorder, "QUANTITY", 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(twentysix), 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(thirtyfive));

    SelectExecutor *date_s_exe      = new SelectExecutorInt( // here should be a pure join
                                            date, "YEAR MONTH NUM", 199401, arrow::compute::CompareOperator::EQUAL);
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    


    std::vector <JoinExecutor*> tree = {date_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}



// WE NEED SELECT ON FACT TABLE HERE
void Query1_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 1.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();


    arrow::NumericScalar<arrow::Int64Type> five(5);
    arrow::NumericScalar<arrow::Int64Type> seven(7);
    arrow::NumericScalar<arrow::Int64Type> twentysix(26);
    arrow::NumericScalar<arrow::Int64Type> thirtyfive(35);
    
    lineorder = SelectBetween(lineorder, "DISCOUNT", 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(five), 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(seven));
    lineorder = SelectBetween(lineorder, "QUANTITY", 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(twentysix), 
                                std::make_shared<arrow::NumericScalar<arrow::Int64Type>>(thirtyfive));

    SelectExecutor *date_s_exe      = new SelectExecutorInt( // here should be a pure join
                                            date, "YEAR MONTH NUM", 199401, arrow::compute::CompareOperator::EQUAL);
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    


    std::vector <JoinExecutor*> tree = {date_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}




// WE NEED SELECT EVERYTHING HERE, PURE JOIN
void Query2_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 2.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    // produce the join tree by constructing the SelectExecutor and JoinExecutor:

    SelectExecutor *date_s_exe      = new SelectExecutorInt( // here should be a pure join
                                            date, "YEAR", 1995, arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *part_s_exe      = new SelectExecutorStr(
                                            part, "CATEGORY", "MFGR#12",arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            supplier, "REGION", "ASIA",arrow::compute::CompareOperator::EQUAL);
    
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {date_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}



// HERE WE NEED STRING BETWEEN
void Query2_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 2.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    // produce the join tree by constructing the SelectExecutor and JoinExecutor:
    std::vector <JoinExecutor*> tree = {};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}

void Query2_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 2.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();




    SelectExecutor *date_s_exe      = new SelectExecutorInt( // here should be a pure join
                                            date, "YEAR", 1995, arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *part_s_exe      = new SelectExecutorStr(
                                            part, "BRAND", "MFGR#2239",arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            supplier, "REGION", "EUROPE",arrow::compute::CompareOperator::EQUAL);
    
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {date_j_exe, part_j_exe, supplier_j_exe};


    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}


void Query3_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();






    SelectExecutor *customer_s_exe      = new SelectExecutorStr( 
                                            customer, "REGION", "ASIA", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *date_s_exe      = new SelectExecutorBetween( 
                                            date, "YEAR", 1992, 1997);
    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            supplier, "REGION", "ASIA",arrow::compute::CompareOperator::EQUAL);
    
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe, "SUPP KEY", "SUPP KEY");

    std::vector <JoinExecutor*> tree = {date_j_exe, customer_j_exe, supplier_j_exe};


    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}


void Query3_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();






    SelectExecutor *customer_s_exe  = new SelectExecutorStr( 
                                            customer, "NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *date_s_exe      = new SelectExecutorBetween( 
                                            date, "YEAR", 1992, 1997);
    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            supplier, "NATION", "UNITED STATES",arrow::compute::CompareOperator::EQUAL);
    
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe, "SUPP KEY", "SUPP KEY");

    std::vector <JoinExecutor*> tree = {date_j_exe, customer_j_exe, supplier_j_exe};



    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}

// NEED TO EVALUATE 'OR' FOR ONE DIM TABLE
void Query3_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();






    // need or
    SelectExecutor *customer_s_exe  = new SelectExecutorStr( 
                                            customer, "NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *date_s_exe      = new SelectExecutorBetween( 
                                            date, "YEAR", 1992, 1997);
    // need or
    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            supplier, "NATION", "UNITED STATES",arrow::compute::CompareOperator::EQUAL);
    
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe, "SUPP KEY", "SUPP KEY");

    std::vector <JoinExecutor*> tree = {date_j_exe, customer_j_exe, supplier_j_exe};


    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}


// NEED TO EVALUATE 'OR' FOR ONE DIM TABLE
void Query3_4(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.4 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();






    // need or
    SelectExecutor *customer_s_exe  = new SelectExecutorStr( 
                                            customer, "NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *date_s_exe      = new SelectExecutorStr( 
                                            date, "YEAR MONTH", "Dec1997", arrow::compute::CompareOperator::EQUAL);
    // need or
    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            supplier, "NATION", "UNITED STATES",arrow::compute::CompareOperator::EQUAL);
    
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe, "DATE KEY", "ORDER DATE");
    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe, "SUPP KEY", "SUPP KEY");

    std::vector <JoinExecutor*> tree = {date_j_exe, customer_j_exe, supplier_j_exe};


    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}

// NEED TO EVALUATE 'OR'
void Query4_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 4.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();





    // produce the join tree by constructing the SelectExecutor and JoinExecutor:
    std::vector <JoinExecutor*> tree = {};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}

// NEED TO EVALUATE 'OR'
void Query4_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 4.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();





    // produce the join tree by constructing the SelectExecutor and JoinExecutor:
    std::vector <JoinExecutor*> tree = {};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}

// NEED TO EVALUATE 'OR'
void Query4_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 4.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();





    // produce the join tree by constructing the SelectExecutor and JoinExecutor:
    std::vector <JoinExecutor*> tree = {};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}



