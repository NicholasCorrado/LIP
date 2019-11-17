//
// Created by Nicholas Corrado on 11/15/19.
//

#ifndef CSV_TO_ARROW_CPP_QUERIES_H
#define CSV_TO_ARROW_CPP_QUERIES_H

#include <iostream>
#include <map>

#include "BuildFilter.h"
#include "Join.h"
#include "util/util.h"
#include "JoinExecutor.h"

enum ALG{
	HASH_JOIN,
	LIP_STANDARD
};


int main_nick();
int main_xiating();

/*
void RunAllQueries(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);

void Query1_1(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query1_2(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);

void Query1_3(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);

void Query2_1(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);

void Query2_2(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);

void Query2_3(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query3_1(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query3_2(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query3_3(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);



void Query3_4(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query4_1(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query4_2(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);


void Query4_3(std::shared_ptr <arrow::Table> customer, 
				std::shared_ptr <arrow::Table> date,
				std::shared_ptr <arrow::Table> lineorder,
				std::shared_ptr <arrow::Table> part,
				std::shared_ptr <arrow::Table> supplier,
				int alg_flag);

*/
#endif //CSV_TO_ARROW_CPP_QUERIES_H



// std::vector <std::string> customer_schema = {"CUST KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE",
//                                                  "MKT SEGMENT"};
//     std::vector <std::string> date_schema = {"DATE KEY", "DATE", "DAY OF WEEK", "MONTH", "YEAR", "YEAR MONTH NUM",
//                                              "YEAR MONTH", "DAY NUM IN WEEK", "DAY NUM IN MONTH", "MONTH NUM IN YEAR",
//                                              "DAY NUM IN YEAR",
//                                              "WEEK NUM IN YEAR", "SELLING SEASON", "LAST DAY IN WEEK FL",
//                                              "LAST DAT IN MONTH FL", "HOLIDAY FL", "WEEKDAY FL",};
//     std::vector <std::string> lineorder_schema = {"ORDER KEY", "LINE NUMBER", "CUST KEY", "PART KEY", "SUPP KEY",
//                                                   "ORDER DATE", "ORD PRIORITY", "SHIP PRIORITY", "QUANTITY",
//                                                   "EXTENDED PRICE", "ORD TOTAL PRICE", "DISCOUNT", "REVENUE",
//                                                   "SUPPLY COST", "TAX", "COMMIT DATE", "SHIP MODE"};
//     std::vector <std::string> part_schema = {"PART KEY", "NAME", "MFGR", "CATEGORY", "BRAND", "COLOR", "TYPE", "SIZE",
//                                              "CONTAINER"};
//     std::vector <std::string> supplier_schema = {"SUPP KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE"};

