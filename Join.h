#ifndef JOIN_H
#define JOIN_H


#include <iostream>
#include <arrow/api.h>
#include "select.h"
#include "JoinExecutor.h"
#include <map>

std::shared_ptr<arrow::Table> HashJoin(std::shared_ptr<arrow::Table> left_table, 
												std::string left_field, 
												std::shared_ptr<arrow::Table> right_table, 
												std::string right_field);

std::shared_ptr<arrow::Table> EvaluateJoinTree(std::shared_ptr<arrow::Table> fact_table, 
												std::vector<JoinExecutor*> joinExecutors);


std::shared_ptr<arrow::Table> EvaluateJoinTreeLIP(std::shared_ptr<arrow::Table> fact_table, 
												std::vector<JoinExecutor*> joinExecutors);

std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPXiating(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors);


std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPResurrection(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors);

std::shared_ptr<arrow::Table> EvaluateJoinTreeLIPK(std::shared_ptr<arrow::Table> fact_table, 
                                                std::vector<JoinExecutor*> joinExecutors);


#endif