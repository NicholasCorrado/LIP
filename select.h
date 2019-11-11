//
// Created by Nicholas Corrado on 11/9/19.
//

#ifndef CSV_TO_ARROW_CPP_SELECT_H
#define CSV_TO_ARROW_CPP_SELECT_H

#endif //CSV_TO_ARROW_CPP_SELECT_H

enum Operator {
    EQUAL,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
};

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
std::shared_ptr<arrow::Table> Select(std::shared_ptr<arrow::Table> table, std::string select_field, std::string value, Operator op);
std::shared_ptr<arrow::Table> Select(std::shared_ptr<arrow::Table> table, std::string select_field, long long value, Operator op);

/// \brief Copy a row from one record batch into a batch builder. Used to
///
/// \param row row index of a row we want to copy from in_batch
/// \param in_batch record batch from which we want to copy a row
/// \param out_batch_builder batch builder to which we are adding a row.
void AddRowToRecordBatch(int row,  std::shared_ptr<arrow::RecordBatch>& in_batch, std::unique_ptr<arrow::RecordBatchBuilder>& out_batch_builder);

/// \brief Evaluate the following statement: data [op] value
///
/// \param data data from a column
/// \param value value to which we are comparing data
/// \param op
/// \return true if (data [op] value) == true, otherwise false
template <typename T>
bool EvaluatePredicate(T data, T value, Operator op);

void EvaluateStatus(arrow::Status status);

void PrintTable(std::shared_ptr<arrow::Table> table);