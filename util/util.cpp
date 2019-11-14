//
// Created by Nicholas Corrado on 11/13/19.
//

#include <iostream>
#include "util.h"

void EvaluateStatus(arrow::Status status) {
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
}
void PrintTable(std::shared_ptr<arrow::Table> table) {

    auto* reader = new arrow::TableBatchReader(*table);
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status status;

    status = reader->ReadNext(&batch);
    while (batch != nullptr) {
        for (int row=0; row < batch->num_rows(); row++) {
            for (int i = 0; i < batch->schema()->num_fields(); i++) {

                int type = batch->schema()->field(i)->type()->id();

                switch (type) {
                    case arrow::Type::type::STRING: {
                        auto col = std::static_pointer_cast<arrow::StringArray>(batch->column(i));
                        std::cout << col->GetString(row) << "\t";
                        break;
                    }
                    case arrow::Type::type::INT64: {
                        auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(i));
                        std::cout << col->Value(row) << "\t";
                        break;
                    }
                }
            }
            std::cout << std::endl;
        }
        status = reader->ReadNext(&batch);
    }
}