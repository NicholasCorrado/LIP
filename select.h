//
// Created by Nicholas Corrado on 11/9/19.
//

#ifndef SELECT_H
#define SELECT_H

#include <arrow/api.h>
#include <arrow/compute/api.h>


/// \brief Performs a simple select query of the following form:
/// SELECT *
/// FROM table
/// WHERE select_field [op] value
///
/// \param table
/// \param select_field name of the column in the select predicate
/// \param value value
/// \param op comparison operator in the select predicate
///
/// \return the result of the query wrapped in std::shared_ptr<arrow::Table>

arrow::compute::Datum* Select2(std::shared_ptr<arrow::Table> table,
                                     std::string select_field,
                                     std::shared_ptr<arrow::Scalar> value,
                                     arrow::compute::CompareOperator op);

std::shared_ptr<arrow::Table> Select(std::shared_ptr<arrow::Table> table, 
										std::string select_field, 
										std::shared_ptr<arrow::Scalar> value, 
										arrow::compute::CompareOperator op);

std::shared_ptr<arrow::Table> SelectBetween(std::shared_ptr<arrow::Table> table, 
												std::string select_field, 
												std::shared_ptr<arrow::Scalar> lo,
												std::shared_ptr<arrow::Scalar> hi);

std::shared_ptr<arrow::Table> SelectString(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, arrow::compute::CompareOperator op);

template <typename T>
bool EvaluatePredicate(T data, T value, arrow::compute::CompareOperator op);

#endif