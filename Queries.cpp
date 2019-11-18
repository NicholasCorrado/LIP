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
#include "util/util.h"
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


int ui(){

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


    std::cout << "To run a specific query [2.3] or [all]" << std::endl;
    std::cout << "To run main_nick, enter nick" << std::endl;
    std::cout << "To exit, enter exit" << std::endl;
    std::string q;

    while (true) {
        std::cout << ">>> ";
        std::cin >> q;

        if (q == "nick") {
            main_nick();
            return 0;
        }
        if (q == "exit") {
            return 0;
        }

        std::string alg;
        std::cout << "Choose an algorithm [lip] or [hash]\n>>> ";
        std::cin >> alg;


        int alg_flag;

        if (alg == "lip") {
            alg_flag = ALG::LIP_STANDARD;
        } else if (alg == "hash") {
            alg_flag = ALG::HASH_JOIN;
        } else {
            alg_flag = ALG::UNKNOWN;
        }

        if (q == "1.1") {
            Query1_1(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "1.2") {
            Query1_2(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "1.3") {
            Query1_3(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "2.1") {
            Query2_1(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "2.2") {
            Query2_2(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "2.3") {
            Query2_3(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "3.1") {
            Query3_1(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "3.2") {
            Query3_2(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "3.3") {
            Query3_3(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "3.4") {
            Query3_4(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "4.1") {
            Query4_1(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "4.2") {
            Query4_2(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "4.3") {
            Query4_3(customer, date, lineorder, part, supplier, alg_flag);
        } else if (q == "all") {
            RunAllQueries(customer, date, lineorder, part, supplier, alg_flag);
        } else {
            std::cout << "Unknown query entered." << std::endl;

        }
    }
    return 0;
}



int main_nick() {

    std::string file_path_customer  = "benchmark/customer.tbl";
    std::string file_path_date      = "benchmark/date.tbl";
    std::string file_path_lineorder = "benchmark/lineorder.tbl";
    std::string file_path_part      = "benchmark/part.tbl";
    std::string file_path_supplier  = "benchmark/supplier.tbl";


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

    /*
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
    */
    //SelectExecutor* customer_s_exe = new SelectExecutorCompare(customer, "CUST KEY", 1, arrow::compute::CompareOperator::EQUAL);
    //JoinExecutor* j_exe = new JoinExecutor(customer_s_exe, "CUST KEY", "CUST KEY");



    /*
    auto* s_date_1 = new SelectExecutorInt(date, "YEAR", 1992, arrow::compute::CompareOperator::EQUAL);
    auto* s_date_2 = new SelectExecutorInt(date, "YEAR", 1995, arrow::compute::CompareOperator::EQUAL);
    std::vector<SelectExecutor*> children = {s_date_1, s_date_2};
    auto* s_comp = new SelectExecutorComposite(children);

    auto* s_customer = new SelectExecutorInt(customer, "CUST KEY", 1, arrow::compute::CompareOperator::EQUAL);
    //auto* j1 = new JoinExecutor(s_date, "DATE KEY", "ORDER DATE");
    //auto* j2 = new JoinExecutor(s_customer, "CUST KEY", "CUST KEY");
    auto* j = new JoinExecutor(s_comp, "DATE KEY", "ORDER DATE");
    std::shared_ptr<arrow::Table> result;

    result = j->join(lineorder);
    PrintTable(result);
    std::vector<JoinExecutor*> tree = {j};
    //auto result = j1->join(lineorder);
    result = EvaluateJoinTreeLIP(lineorder, tree);
    PrintTable(result);
    std::cout << result->num_rows() << std::endl;
*/

    /*
    std::shared_ptr <arrow::Table> result_table;
    auto* s_date_1 = new SelectExecutorBetween("YEAR", 1992, 1994);
    auto* s_date_2 = new SelectExecutorInt("YEAR", 1992, arrow::compute::CompareOperator::EQUAL);
    auto* s_customer = new SelectExecutorInt("CUST KEY", 2, arrow::compute::CompareOperator::EQUAL);
    auto* s_customer_str = new SelectExecutorStrBetween("REGION", "ASIA", "AMERICA");

    std::vector<SelectExecutor*> s_children = {s_date_1, s_customer_str};
    auto* s_date_tree = new SelectExecutorTree(date, s_date_1);
    auto* s_customer_str_tree = new SelectExecutorTree(customer, s_customer_str);

    auto* j_date = new JoinExecutor(s_date_tree, "DATE KEY", "ORDER DATE");
    auto* j_customer = new JoinExecutor(s_customer_str_tree, "CUST KEY", "CUST KEY");

    std::vector<JoinExecutor*> j = {j_customer, j_date};
    result_table = EvaluateJoinTreeLIP(lineorder, j);
    PrintTable(result_table, 1);
    */
    // To select all tuples, pass a null SelectExecutor to the tree.


    //auto* j_date = new JoinExecutor(s_date_tree, "DATE KEY", "ORDER DATE");
    //auto* j_customer = new JoinExecutor(s_customer_tree, "CUST KEY", "CUST KEY");

    /*
    result_table = s_customer_tree->Select();
    PrintTable(result_table);
    std::cout<<result_table->num_rows()<<std::endl;

    result_table = j_customer->join(lineorder);
    PrintTable(result_table);
    std::cout<<result_table->num_rows()<<std::endl;
    */
    // result_table = EvaluateJoinTree(lineorder,j);
    // PrintTable(result_table, 1);
    
    
    //result_table = j_date->join(lineorder);
    //PrintTable(result_table);
    //std::cout << result_table->num_rows() << std::endl;


    //@TODO: what??????? AHHH okay you can use a select executor for only one query, since the reader is at the end each time you try to reuse it.
    //std::shared_ptr<arrow::Table> t1 = j_customer->join(lineorder);


    //std::shared_ptr<arrow::RecordBatch> b;
    //status = s_customer_tree->reader->Next(&b);
    //std::cout<<"customer len = "  << (b == nullptr) << std::endl;
    //std::shared_ptr<arrow::Table> t2 = j_date->join(lineorder);
    //Instead of selecting all rows for join with no selects, just leave the option to not do a select.
    //auto* j_final = new JoinExecutor(nullptr, j_customer);


    //PrintTable(result_table);
    //std::cout << result_table->num_rows() << std::endl;
//    std::vector<JoinExecutor*> j = {j_customer, j_date};
//    result_table = EvaluateJoinTree(lineorder,j);
//    std::cout<<result_table->num_rows()<<std::endl;
//    PrintTable(result_table);
//
//    return 0;

    RunAllQueries_nick(customer, date, lineorder, part, supplier);
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


    RunAllQueries(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    RunAllQueries(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    

    return 0;
}


void RunAllQueries_nick(std::shared_ptr <arrow::Table> customer,
                   std::shared_ptr <arrow::Table> date,
                   std::shared_ptr <arrow::Table> lineorder,
                   std::shared_ptr <arrow::Table> part,
                   std::shared_ptr <arrow::Table> supplier)
{

    int hashjoin_time = 0;
    int LIPjoin_time = 0;

    hashjoin_time = Query1_1(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query1_1(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query1_2(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query1_2(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query1_3(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query1_3(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;

    hashjoin_time = Query2_1(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query2_1(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query2_2(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query2_2(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query2_3(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query2_3(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;

    hashjoin_time = Query3_1(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query3_1(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query3_2(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query3_2(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query3_3(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query3_3(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query3_4(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query3_4(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;

    hashjoin_time = Query4_1(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query4_1(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query4_2(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query4_2(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
    hashjoin_time = Query4_3(customer, date, lineorder, part, supplier, ALG::HASH_JOIN);
    LIPjoin_time  = Query4_3(customer, date, lineorder, part, supplier, ALG::LIP_STANDARD);
    std::cout<< "DELTA = " << hashjoin_time - LIPjoin_time <<"\n" <<std::endl;
}

void RunAllQueries(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    Query1_1(customer, date, lineorder, part, supplier, alg_flag);
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
     
}


void AlgorithmSwitcher(std::shared_ptr <arrow::Table> lineorder, std::vector<JoinExecutor*> tree, int alg_flag){
    
    std::shared_ptr <arrow::Table> result_table = nullptr;
    switch (alg_flag){
        case ALG::HASH_JOIN:
            result_table = EvaluateJoinTree(lineorder, tree);
            break;
        case ALG::LIP_STANDARD:
            result_table = EvaluateJoinTreeLIP(lineorder, tree);
            break;
        default:
            std::cout << "Unknown algorithm" << std::endl;
            break;
    }
    if (result_table != nullptr)
        std::cout << "Rows " << result_table -> num_rows() << std::endl; 
}



int Query1_1(std::shared_ptr <arrow::Table> customer, 
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

    // WHERE lo_dicount between 1 and 3 and lo_quantity < 25;

    SelectExecutor *date_s_exe = new SelectExecutorInt("YEAR", 1993, arrow::compute::CompareOperator::EQUAL);
    
    // WHERE date = 1993 
    SelectExecutorTree *s_tree = new SelectExecutorTree(date, date_s_exe);

    JoinExecutor *date_j_exe = new JoinExecutor(s_tree, "DATE KEY", "ORDER DATE");
    

    std::vector <JoinExecutor*> tree = {date_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}



// WE NEED SELECT ON FACT TABLE HERE
int Query1_2(std::shared_ptr <arrow::Table> customer, 
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

    // WHERE lo_discount between 4 and 6 AND quantity between 26 and 35

    SelectExecutor *date_s_exe      = new SelectExecutorInt("YEAR MONTH NUM", 199401, arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *s_exe_tree = new SelectExecutorTree(date, date_s_exe);

    JoinExecutor *date_j_exe = new JoinExecutor(s_exe_tree, "DATE KEY", "ORDER DATE");
    


    std::vector <JoinExecutor*> tree = {date_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}



// WE NEED SELECT ON FACT TABLE HERE
int Query1_3(std::shared_ptr <arrow::Table> customer, 
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

    // WHERE discount between 5 and 7 and quantity between 26 and 35

    SelectExecutor *date_s_exe      = new SelectExecutorInt("YEAR MONTH NUM", 199401, arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *s_exe_tree = new SelectExecutorTree(date, date_s_exe);

    JoinExecutor *date_j_exe = new JoinExecutor(s_exe_tree, "DATE KEY", "ORDER DATE");
    


    std::vector <JoinExecutor*> tree = {date_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}




// WE NEED SELECT EVERYTHING HERE, PURE JOIN
int Query2_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 2.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    
    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, nullptr);
    // WHERE DATE CAN BE ANYTHING

    SelectExecutor *part_s_exe      = new SelectExecutorStr(
                                            "CATEGORY", "MFGR#12",arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *part_s_exe_tree = new SelectExecutorTree(part, part_s_exe);
    // WHERE PART.CATEGORY = MFGR#12

    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            "REGION", "AMERICA",arrow::compute::CompareOperator::EQUAL);

    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.REGION = AMERICA


    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe_tree, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {date_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}



// HERE WE NEED STRING BETWEEN
int Query2_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 2.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, nullptr);
    // WHERE DATE CAN BE ANYTHING

    SelectExecutor *part_s_exe      = new SelectExecutorStrBetween(
                                            "BRAND", "MFGR#2221", "MFGR#2228");
    SelectExecutorTree *part_s_exe_tree = new SelectExecutorTree(part, part_s_exe);
    // WHERE PART.BRAND BETWEEN  MFGR#2221 AND MFGR#2228

    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            "REGION", "ASIA", arrow::compute::CompareOperator::EQUAL);

    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.REGION = ASIA


    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe_tree, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {date_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);



    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}


int Query2_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 2.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, nullptr);
    // WHERE DATE CAN BE ANYTHING

    SelectExecutor *part_s_exe      = new SelectExecutorStr(
                                            "BRAND", "MFGR#2239", arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *part_s_exe_tree = new SelectExecutorTree(part, part_s_exe);
    // WHERE PART.BRAND = MFGR#2239

    SelectExecutor *supplier_s_exe  = new SelectExecutorStr(
                                            "REGION", "EUROPE",arrow::compute::CompareOperator::EQUAL);

    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.REGION = EUROPE


    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe_tree, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {date_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);


    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}


int Query3_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();


    SelectExecutor *customer_s_exe = new SelectExecutorStr("REGION", "ASIA", arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe);
    // WHERE CUSTOMER.REGION = ASIA

    SelectExecutor *supplier_s_exe      = new SelectExecutorStr(
                                            "REGION", "ASIA", arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.REGION = ASIA

    SelectExecutor *date_s_exe  = new SelectExecutorBetween(
                                            "YEAR", 1992, 1997);
    
    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe);
    // WHERE DATE.YEAR >= 1992 AND DATE.YEAR <= 1997

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {customer_j_exe, date_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);




    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}


int Query3_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    SelectExecutor *customer_s_exe = new SelectExecutorStr("NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe);
    // WHERE CUSTOMER.NATION = UNITED STATES

    SelectExecutor *supplier_s_exe      = new SelectExecutorStr(
                                            "REGION", "ASIA", arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER REGION = ASIA

    SelectExecutor *date_s_exe  = new SelectExecutorBetween(
                                            "YEAR", 1992, 1997);
    
    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe);
    // WHERE DATE.YEAR >= 1992 AND DATE.YEAR <= 1997

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {customer_j_exe, date_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}


// NEED TO EVALUATE 'OR' FOR ONE DIM TABLE
int Query3_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();




    SelectExecutor *customer_s_exe_ki1 = new SelectExecutorStr("CITY", "UNITED KI1", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *customer_s_exe_ki5 = new SelectExecutorStr("CITY", "UNITED KI5", arrow::compute::CompareOperator::EQUAL);
    
    std::vector<SelectExecutor*> children_customer{customer_s_exe_ki1, customer_s_exe_ki5};
    SelectExecutor* customer_s_exe_comp = new SelectExecutorComposite(children_customer);
    
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe_comp);
    // WHERE CUSTOMER.CITY = UNITED KI1 OR UNITED KI5

    SelectExecutor *supplier_s_exe_ki1 = new SelectExecutorStr("CITY", "UNITED KI1", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *supplier_s_exe_ki5 = new SelectExecutorStr("CITY", "UNITED KI5", arrow::compute::CompareOperator::EQUAL);
    
    std::vector<SelectExecutor*> children_supplier{supplier_s_exe_ki1, supplier_s_exe_ki5};
    SelectExecutor* supplier_s_exe_comp = new SelectExecutorComposite(children_supplier);

    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe_comp);
    // WHERE SUPPLIER CITY = UNITED KI1 OR UNITED KI5

    SelectExecutor *date_s_exe  = new SelectExecutorBetween(
                                            "YEAR", 1992, 1997);
    
    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe);
    // WHERE DATE.YEAR >= 1992 AND DATE.YEAR <= 1997

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {customer_j_exe, date_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);




    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}



// NEED TO EVALUATE 'OR' FOR ONE DIM TABLE
int Query3_4(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 3.4 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();


    SelectExecutor *customer_s_exe_ki1 = new SelectExecutorStr("CITY", "UNITED KI1", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *customer_s_exe_ki5 = new SelectExecutorStr("CITY", "UNITED KI5", arrow::compute::CompareOperator::EQUAL);
    
    std::vector<SelectExecutor*> children_customer{customer_s_exe_ki1, customer_s_exe_ki5};
    SelectExecutor* customer_s_exe_comp = new SelectExecutorComposite(children_customer);
    
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe_comp);
    // WHERE CUSTOMER.CITY = UNITED KI1 OR UNITED KI5

    SelectExecutor *supplier_s_exe_ki1 = new SelectExecutorStr("CITY", "UNITED KI1", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *supplier_s_exe_ki5 = new SelectExecutorStr("CITY", "UNITED KI5", arrow::compute::CompareOperator::EQUAL);
    
    std::vector<SelectExecutor*> children_supplier{supplier_s_exe_ki1, supplier_s_exe_ki5};
    SelectExecutor* supplier_s_exe_comp = new SelectExecutorComposite(children_supplier);

    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe_comp);
    // WHERE SUPPLIER CITY = UNITED KI1 OR UNITED KI5

    SelectExecutor *date_s_exe  = new SelectExecutorStr(
                                            "YEAR MONTH", "Dec1997", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe);
    // WHERE DATE.YEAR MON = Dec1997

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {customer_j_exe, date_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);


    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}



// NEED TO EVALUATE 'OR'
int Query4_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 4.1 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    SelectExecutor *customer_s_exe = new SelectExecutorStr("REGION", "AMERICA", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe);
    // WHERE CUSTOMER.REGION = AMERICA

    SelectExecutor *supplier_s_exe = new SelectExecutorStr("REGION", "AMERICA", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.REGION = AMERICA

    SelectExecutor *part_s_exe_mfgr1  = new SelectExecutorStr(
                                            "MFGR", "MFGR#1", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *part_s_exe_mfgr2  = new SelectExecutorStr(
                                            "MFGR", "MFGR#2", arrow::compute::CompareOperator::EQUAL);
    std::vector<SelectExecutor*> part_children{part_s_exe_mfgr1, part_s_exe_mfgr2};
    SelectExecutor *part_s_exe = new SelectExecutorComposite(part_children);

    SelectExecutorTree *part_s_exe_tree = new SelectExecutorTree(part, part_s_exe);
    // WHERE PART.MFGR = MFGR#1 OR MFGR#2

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe_tree, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {customer_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);



    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}





// NEED TO EVALUATE 'OR'
int Query4_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 4.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();


    SelectExecutor *customer_s_exe = new SelectExecutorStr("REGION", "AMERICA", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe);
    // WHERE CUSTOMER.REGION = AMERICA

    SelectExecutor *supplier_s_exe = new SelectExecutorStr("REGION", "AMERICA", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.REGION = AMERICA

    SelectExecutor *date_s_exe_97  = new SelectExecutorInt(
                                            "YEAR", 1997, arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *date_s_exe_98  = new SelectExecutorInt(
                                            "YEAR", 1998, arrow::compute::CompareOperator::EQUAL);

    std::vector<SelectExecutor*> date_children{date_s_exe_97, date_s_exe_98};
    SelectExecutor *date_s_exe_comp = new SelectExecutorComposite(date_children);

    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe_comp);
    // WHERE DATE.YEAR = 1997 or 1998


    SelectExecutor *part_s_exe_mfgr1  = new SelectExecutorStr(
                                            "MFGR", "MFGR#1", arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *part_s_exe_mfgr2  = new SelectExecutorStr(
                                            "MFGR", "MFGR#2", arrow::compute::CompareOperator::EQUAL);
    std::vector<SelectExecutor*> part_children{part_s_exe_mfgr1, part_s_exe_mfgr2};
    SelectExecutor *part_s_exe = new SelectExecutorComposite(part_children);

    SelectExecutorTree *part_s_exe_tree = new SelectExecutorTree(part, part_s_exe);
    // WHERE PART.MFGR = MFGR#1 OR MFGR#2

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe_tree, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "SUPP KEY", "SUPP KEY");
    

    std::vector <JoinExecutor*> tree = {customer_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);


    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}


// NEED TO EVALUATE 'OR'
int Query4_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag)
{
    std::cout << "Running query 4.3 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();


    SelectExecutor *customer_s_exe = new SelectExecutorStr("REGION", "AMERICA", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe);
    // WHERE CUSTOMER.REGION = AMERICA

    SelectExecutor *supplier_s_exe = new SelectExecutorStr("NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER.NATION = UNITED STATES

    SelectExecutor *date_s_exe_97  = new SelectExecutorInt(
                                            "YEAR", 1997, arrow::compute::CompareOperator::EQUAL);
    SelectExecutor *date_s_exe_98  = new SelectExecutorInt(
                                            "YEAR", 1998, arrow::compute::CompareOperator::EQUAL);

    std::vector<SelectExecutor*> date_children{date_s_exe_97, date_s_exe_98};
    SelectExecutor *date_s_exe_comp = new SelectExecutorComposite(date_children);

    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe_comp);
    // WHERE DATE.YEAR = 1997 or 1998


    SelectExecutor *part_s_exe_mfgr14  = new SelectExecutorStr(
                                            "CATEGORY", "MFGR#14", arrow::compute::CompareOperator::EQUAL);
    
    SelectExecutorTree *part_s_exe_tree = new SelectExecutorTree(part, part_s_exe_mfgr14);
    // WHERE PART.MFGR = MFGR#14

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *part_j_exe = new JoinExecutor(part_s_exe_tree, "PART KEY", "PART KEY");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "SUPP KEY", "SUPP KEY");
    

    std::vector <JoinExecutor*> tree = {customer_j_exe, part_j_exe, supplier_j_exe};

    AlgorithmSwitcher(lineorder, tree, alg_flag);



    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "RunningTime " << duration.count() << std::endl;

    return duration.count();
}



