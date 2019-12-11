//
// Created by Nicholas Corrado on 11/15/19.
//



#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/stl.h>
#include <chrono>
#include "Queries.h"
#include "../select.h"
#include "../util/util.h"

int k = -1;

int run(std::string q, std::string alg, std::string skew, std::string SF, bool enum_flag) {


    std::string file_path_customer;
    std::string file_path_date;
    std::string file_path_lineorder;
    std::string file_path_part;
    std::string file_path_supplier;

    file_path_customer  = "./benchmarks/benchmark-" + SF + "/customer.tbl";
    file_path_date      = "./benchmarks/benchmark-" + SF + "/date.tbl";
    file_path_lineorder = "./benchmarks/benchmark-" + SF + "/lineorder.tbl";
    file_path_part      = "./benchmarks/benchmark-" + SF + "/part.tbl";
    file_path_supplier  = "./benchmarks/benchmark-" + SF + "/supplier.tbl";

    if (skew.rfind("skew-", 0) == 0) {
        file_path_lineorder = "./benchmarks/benchmark-skew/lineorder-" + skew.substr(5) + ".tbl";
//        file_path_customer  = "./benchmarks/benchmark-skew/customer.tbl";
//        file_path_date      = "./benchmarks/benchmark-skew/date.tbl";
//        file_path_part      = "./benchmarks/benchmark-skew/part.tbl";
//        file_path_supplier  = "./benchmarks/benchmark-skew/supplier.tbl";
    }
    else {
        file_path_customer  = "./benchmarks/benchmark-" + SF + "/customer.tbl";
        file_path_date      = "./benchmarks/benchmark-" + SF + "/date.tbl";
        file_path_lineorder = "./benchmarks/benchmark-" + SF + "/lineorder.tbl";
        file_path_part      = "./benchmarks/benchmark-" + SF + "/part.tbl";
        file_path_supplier  = "./benchmarks/benchmark-" + SF + "/supplier.tbl";
    }

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

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    std::shared_ptr<arrow::Table> customer    = build_table(file_path_customer,  pool, customer_schema);
    std::shared_ptr<arrow::Table> date        = build_table(file_path_date,      pool, date_schema);
    std::shared_ptr<arrow::Table> lineorder   = build_table(file_path_lineorder, pool, lineorder_schema);
    std::shared_ptr<arrow::Table> part        = build_table(file_path_part,      pool, part_schema);
    std::shared_ptr<arrow::Table> supplier    = build_table(file_path_supplier,  pool, supplier_schema);

    int alg_flag;

    if (alg == "lip") {
        alg_flag = ALG::LIP_STANDARD;
    } else if (alg == "hash") {
        alg_flag = ALG::HASH_JOIN;
    } else if (alg == "xiating") {
        alg_flag = ALG::LIP_XIATING;
    } else if (alg == "resur") {
        alg_flag = ALG::LIP_RESURRECTION;
    } else if (alg.rfind("lip",0) == 0) {
        k = std::stoi(alg.substr(3));
        alg_flag = ALG::LIP_K;
    } else if (alg == "time") {
        alg_flag = ALG::TIME;
    } else {
        alg_flag = ALG::UNKNOWN;
    }

    if (q == "1.1") {
        Query1_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "1.2") {
        Query1_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "1.3") {
        Query1_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "2.1") {
        Query2_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "2.2") {
        Query2_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "2.3") {
        Query2_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "3.1") {
        Query3_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "3.2") {
        Query3_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "3.3") {
        Query3_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "3.4") {
        Query3_4(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "4.1") {
        Query4_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "4.2") {
        Query4_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "4.3") {
        Query4_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else if (q == "dim") {
        main_n_dim(alg_flag, enum_flag);
    } else if (q == "all") {
        RunAllQueries(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    } else {
        std::cout << "Unknown query entered." << std::endl;
    }

    return 0;
}


int ui(){
    while(true){
        std::cout << "To run a specific query [2.3] or [all]" << std::endl;
        std::cout << "To run main_nick, enter nick" << std::endl;
        std::string q;
        std::cout << ">>> ";
        std::cin >> q;

        if (q == "quit" || q == "exit"){
            break;
        }

        std::string alg;
        std::cout << "Choose an algorithm [lip] or [hash]\n>>> ";
        std::cin >> alg;

        std::string skew;
        std::cout << "skew = ?\n>>> ";
        std::cin >> skew;

        std::string SF;
        std::cout << "SF = ?\n>>> ";
        std::cin >> SF;

        std::string s_enum_flag;
        std::cout << "Enumerate all plans? [y/n]\n>>> ";
        std::cin >> s_enum_flag;

        bool enum_flag = (s_enum_flag == "y" || s_enum_flag == "Y");
        run(q, alg, skew, SF, enum_flag);
        
    }
    return 0;
}



void RunAllQueries(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
{
    Query1_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query1_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query1_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    

    Query2_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query2_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag); 
    Query2_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    

    Query3_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query3_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query3_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query3_4(customer, date, lineorder, part, supplier, alg_flag, enum_flag);

    
    Query4_1(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query4_2(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
    Query4_3(customer, date, lineorder, part, supplier, alg_flag, enum_flag);
     
}


void RunAllPlans(std::shared_ptr <arrow::Table> lineorder, std::vector<JoinExecutor*> tree, int alg_flag){
    std::vector<JoinExecutor*> curTree;
    std::map<JoinExecutor*, bool> map;
    for(int i = 0; i < tree.size(); i++){
        map[tree[i]] = false;
    }
    RunAllPlans_util(lineorder, tree, alg_flag, curTree, map);
}

void RunAllPlans_util(std::shared_ptr <arrow::Table> lineorder, 
                        std::vector<JoinExecutor*> tree, 
                        int alg_flag, 
                        std::vector<JoinExecutor*> curTree, 
                        std::map<JoinExecutor*, bool> map){
    if(curTree.size() >= tree.size()){
        AlgorithmSwitcher(lineorder, curTree, alg_flag);
        return;
    }
    for(int i = 0; i < tree.size(); i++){
        if (!map[tree[i]]){
            // first add it
            curTree.push_back(tree[i]); 
            map[tree[i]] = true;

            RunAllPlans_util(lineorder,
                                tree,
                                alg_flag,
                                curTree,
                                map);

            // then remove it
            curTree.pop_back();
            map[tree[i]] = false;
        }
    }
}



void AlgorithmSwitcher(std::shared_ptr <arrow::Table> lineorder, std::vector<JoinExecutor*> tree, int alg_flag){
    auto start = std::chrono::high_resolution_clock::now();
    std::shared_ptr <arrow::Table> result_table = nullptr;
    switch (alg_flag){
        case ALG::HASH_JOIN:
            result_table = EvaluateJoinTree(lineorder, tree);
            break;
        case ALG::LIP_STANDARD:
            result_table = EvaluateJoinTreeLIP(lineorder, tree);
            break;
        case ALG::LIP_XIATING:
            result_table = EvaluateJoinTreeLIPXiating(lineorder, tree);
            break;
        case ALG::LIP_RESURRECTION:
            result_table = EvaluateJoinTreeLIPResurrection(lineorder, tree);
            break;
        case ALG::LIP_K:
            result_table = EvaluateJoinTreeLIPK(lineorder, tree, k);
            break;
        case ALG::TIME:
            result_table = EvaluateJoinTreeLIPTime(lineorder, tree);
            break;
        default:
            std::cout << "Unknown algorithm" << std::endl;
            break;
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    
    int numOfRows = result_table != nullptr ? result_table -> num_rows() : 0;
//PrintTable(result_table, 0);
    std::cout << "Rows " << numOfRows << std::endl;
    std::cout << "RunningTime " << duration.count() << std::endl;
    //std::cout << duration.count() << std::endl;
}



int Query1_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
{


    std::cout << "Running query 1.1 ..." << std::endl;
    
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

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    
    return 0;
}


int Query1_1_enum(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
{
    std::cout << "Running query 1.1 ..." << std::endl;
    
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

    return 0;
}

// WE NEED SELECT ON FACT TABLE HERE
int Query1_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}



// WE NEED SELECT ON FACT TABLE HERE
int Query1_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}




// WE NEED SELECT EVERYTHING HERE, PURE JOIN
int Query2_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {part_j_exe, supplier_j_exe, date_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}



// HERE WE NEED STRING BETWEEN
int Query2_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {part_j_exe, supplier_j_exe, date_j_exe};
    
    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}


int Query2_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {part_j_exe, supplier_j_exe, date_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}


int Query3_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {customer_j_exe, supplier_j_exe, date_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}


int Query3_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
{
    std::cout << "Running query 3.2 ..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();



    SelectExecutor *customer_s_exe = new SelectExecutorStr("NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);
    SelectExecutorTree *customer_s_exe_tree = new SelectExecutorTree(customer, customer_s_exe);
    // WHERE CUSTOMER.NATION = UNITED STATES

    //@TODO: HERE. THIS SHOULD BE "NATION" == "UNITED STATES"
//    SelectExecutor *supplier_s_exe      = new SelectExecutorStr(
//                                            "REGION", "ASIA", arrow::compute::CompareOperator::EQUAL);

    SelectExecutor *supplier_s_exe      = new SelectExecutorStr(
            "NATION", "UNITED STATES", arrow::compute::CompareOperator::EQUAL);

    SelectExecutorTree *supplier_s_exe_tree = new SelectExecutorTree(supplier, supplier_s_exe);
    // WHERE SUPPLIER REGION = ASIA

    SelectExecutor *date_s_exe  = new SelectExecutorBetween(
                                            "YEAR", 1992, 1997);
    
    SelectExecutorTree *date_s_exe_tree = new SelectExecutorTree(date, date_s_exe);
    // WHERE DATE.YEAR >= 1992 AND DATE.YEAR <= 1997

    JoinExecutor *customer_j_exe = new JoinExecutor(customer_s_exe_tree, "CUST KEY", "CUST KEY");
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    JoinExecutor *supplier_j_exe = new JoinExecutor(supplier_s_exe_tree, "SUPP KEY", "SUPP KEY");


    std::vector <JoinExecutor*> tree = {customer_j_exe, supplier_j_exe, date_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}


// NEED TO EVALUATE 'OR' FOR ONE DIM TABLE
int Query3_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {supplier_j_exe, customer_j_exe, date_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}



// NEED TO EVALUATE 'OR' FOR ONE DIM TABLE
int Query3_4(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {supplier_j_exe, date_j_exe, customer_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}



// NEED TO EVALUATE 'OR'
int Query4_1(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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


    std::vector <JoinExecutor*> tree = {supplier_j_exe, customer_j_exe,  part_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}





// NEED TO EVALUATE 'OR'
int Query4_2(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    

    //std::vector <JoinExecutor*> tree = {customer_j_exe, date_j_exe, supplier_j_exe, part_j_exe};
    std::vector <JoinExecutor*> tree = {date_j_exe, part_j_exe, supplier_j_exe, customer_j_exe};

    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }
    return 0;
}


// NEED TO EVALUATE 'OR'
int Query4_3(std::shared_ptr <arrow::Table> customer, 
                std::shared_ptr <arrow::Table> date,
                std::shared_ptr <arrow::Table> lineorder,
                std::shared_ptr <arrow::Table> part,
                std::shared_ptr <arrow::Table> supplier,
                int alg_flag,
                bool enum_flag)
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
    JoinExecutor *date_j_exe = new JoinExecutor(date_s_exe_tree, "DATE KEY", "ORDER DATE");
    

    //std::vector <JoinExecutor*> tree = {customer_j_exe, part_j_exe, supplier_j_exe, date_j_exe};
    std::vector <JoinExecutor*> tree = {supplier_j_exe, date_j_exe, part_j_exe, customer_j_exe};
    if (enum_flag){
        RunAllPlans(lineorder, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(lineorder, tree, alg_flag); 
    }

    return 0;
}




void main_n_dim(int alg_flag, bool enum_flag){
    std::string path_base  = "./benchmarks/benchmark-n-dim/";

    std::vector<std::vector<std::string>> dim_schemas;
    std::vector<std::string> fact_schema;

    for(int i = 0; i < 10; i++){
        std::string schema = "K" + std::to_string(i);

        fact_schema.push_back(schema);

        std::vector<std::string> schema_v = {schema};
        dim_schemas.push_back(schema_v);
    }

    std::vector<std::shared_ptr<arrow::Table>> dim_tables;

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    for(int i = 0; i < 10; i++){
        std::string filename = "dim" + std::to_string(i) + ".tbl";

        std::shared_ptr<arrow::Table> table = build_table(path_base + filename, pool, dim_schemas[i]);

        dim_tables.push_back(table);
    }

    std::shared_ptr<arrow::Table> fact_table = build_table(path_base + "fact.tbl", pool, fact_schema);

    Query_n_dim(fact_table, dim_tables, alg_flag, enum_flag);
}


int Query_n_dim(std::shared_ptr<arrow::Table> fact_table, 
                    std::vector<std::shared_ptr<arrow::Table>> dim_tables, 
                    int alg_flag, bool enum_flag){

    std::cout << "Running query n-dim ..." << std::endl;

    std::vector<JoinExecutor*> tree;

    for(int i = 0; i < 10; i++){
        std::string key = "K" + std::to_string(i);

        SelectExecutorTree *s_exe_t_i = new SelectExecutorTree(dim_tables[i], nullptr);
        JoinExecutor* j_exe = new JoinExecutor(s_exe_t_i, key, key);
        tree.push_back(j_exe);
    }

    
    if (enum_flag){
        RunAllPlans(fact_table, tree, alg_flag);
    }
    else{
        AlgorithmSwitcher(fact_table, tree, alg_flag); 
    }
    return 0;

}

