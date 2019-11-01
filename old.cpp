#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/csv/api.h>
#include <fstream>
#include <stdio.h>
#include <arrow/dataset/api.h>

void read_lineorder();
void read_lineorder2();
//void build_table(const std::string&, arrow::MemoryPool *, std::shared_ptr<arrow::Table> );
//std::shared_ptr<arrow::Table> build_table(const std::string&, arrow::MemoryPool *);


std::shared_ptr<arrow::Table> build_table_old(const std::string&, arrow::MemoryPool *, std::vector<std::string> );

int main_OLD() {

    //std::string file_path_customer  = "/Users/corrado/ClionProjects/csv_to_arrow/test.csv";
    std::string file_path_customer  = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/customer.tbl";
    std::string file_path_date      = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/date.tbl";
    std::string file_path_lineorder = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/lineorder.tbl";
    std::string file_path_part      = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/part.tbl";
    std::string file_path_supplier  = "/Users/corrado/SQL-benchmark-data-generator/ssbgen/supplier.tbl";


    std::vector<std::string> customer_schema    = {"CUST KEY", "NAME", "ADDRESS", "CITY", "NATION", "REGION", "PHONE",
                                                   "MKT SEGMENT"};
    std::vector<std::string> date_schema        = {"DATE KEY", "DATE", "DAY OF WEEK", "MONTH", "YEAR", "YEAR MONTH NUM",
                                                   "YEAR MONTH", "DAY NUM WEEK", "DAY NUM MONTH", "MONTH NUM IN YEAR",
                                                   "WEEK NUM IN YEAR", "SELLING SEASON", "LAST DAT IN MONTH FL",
                                                   "HOLIDAY FL", "WEEKDAY FL", "DAY NUM YEAR"};
    std::vector<std::string> lineorder_schema   = {};
    std::vector<std::string> part_schema        = {};
    std::vector<std::string> supplier_schema    = {};

    std::shared_ptr<arrow::Table> customer;
    std::shared_ptr<arrow::Table> date;
    std::shared_ptr<arrow::Table> lineorder;
    std::shared_ptr<arrow::Table> part;
    std::shared_ptr<arrow::Table> supplier;

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    customer    = build_table(file_path_customer,  pool, customer_schema);
    /*
    date        = build_table(file_path_date,      pool, date_schema);
    lineorder   = build_table(file_path_lineorder, pool, lineorder_schema);
    part        = build_table(file_path_part,      pool, part_schema);
    supplier    = build_table(file_path_supplier,  pool, supplier_schema);
    */

    std::cout<<"customer->num_rows() = " << customer->num_rows() << std::endl;
    /*
    std::cout<<"date->num_rows() = " << date->num_rows() << std::endl;
    std::cout<<"lineorder->num_rows() = " << lineorder->num_rows() << std::endl;
    std::cout<<"part->num_rows() = " << part->num_rows() << std::endl;
    std::cout<<"supplier->num_rows() = " << supplier->num_rows() << std::endl;
    */
    // last column


    std::shared_ptr<arrow::ChunkedArray> col = customer->column(7);
    int count = 0;
    // select
    for (int i=0; i<col->num_chunks(); i++) {
        std::shared_ptr<arrow::Array> chunk = col->chunk(i);

        for (int j=0; j<chunk->length(); j++) {
            std::shared_ptr<arrow::StringArray> values = std::static_pointer_cast<arrow::StringArray>(chunk);
            if (values->GetString(j) == "AUTOMOBILE") {
                count++;
                //std::cout << values->GetString(j) << std::endl;

            }

        }
    }

    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::TableBatchReader tbr(*customer);
    arrow::Status status;

    // second loop condition checks for null pointers.
    // ReadNext() returns null if we read the last record batch, which is totally different from arrow::Status and thus
    // deserves its own condition.
    std::shared_ptr<arrow::Table> result;
    std::shared_ptr<arrow::Field> f1 = arrow::field("CUST KEY",arrow::utf8());
    std::shared_ptr<arrow::Field> f2 = arrow::field("NAME",arrow::utf8());
    std::shared_ptr<arrow::Schema> result_schema = arrow::schema({f1,f2});


    //std::vector<std::shared_ptr<arrow::Array>> cols;
    std::shared_ptr<arrow::Array > a1;
    std::shared_ptr<arrow::Array > a2;
    //std::shared_ptr<arrow::UInt64Builder > c1;
    //std::shared_ptr<arrow::StringBuilder> c2;
    arrow::UInt64Builder c1;
    arrow::StringBuilder c2;

    //arrow::RecordBatchBuilder::
    int count2 = 0;
    while (tbr.ReadNext(&record_batch).ok() && record_batch) {
        std::cout<< "num rows in batch = " << record_batch->num_rows() << std::endl;
        std::shared_ptr<arrow::StringArray> data = std::static_pointer_cast<arrow::StringArray>(record_batch->column(7));
        int length = data->length();
        for (int i=0; i<length; i++) {
            if (data->GetString(i) == "AUTOMOBILE") {
                auto array1 = std::static_pointer_cast<arrow::UInt64Array>(record_batch->column(0));
                auto array2 = std::static_pointer_cast<arrow::StringArray>(record_batch->column(1));
                c1.Append(array1->Value(i));
                c2.Append(array2->GetString(i));
                // Okay, so setting columns to null won't help us if we have no null values in the first place...
                //const uint8_t* bmp = data->null_bitmap_data();
                //std::cout<<"bitmap " << bmp[0] << std::endl;
                //std::shared_ptr<arrow::Buffer> bmp = data->null_bitmap();
                //if (!bmp->mutable_data()) std::cout<<"NULL" <<std::endl;
                //std::cout<<bmp->ToString()<<std::endl;
                //uint8_t* raw = bmp->mutable_data();
                //raw[i]=0;
                //std::cout<<"bitmap " << bmp->mutable_data()[0] << std::endl;
                count2++;
            }
        }
        std::cout<<record_batch->column(7)->null_count() << std::endl;
    }
    c1.Finish(&a1);
    c2.Finish(&a2);
    result->Make(result_schema,{a1, a2});

    //std::cout<<customer->column(7)->null_count() << std::endl;
    //std::cout<<count<< " " << count2 << std::endl;
    //std::cout<<a1->ToString()<< std::endl;
    //do {
        //std::cout<< "num rows in batch = " << record_batch->num_rows() << std::endl;
        //status = tbr.ReadNext(&record_batch);
    //} while(status.ok());

    std::cout<<"TEST"<<std::endl;


    //FILE *out;
    //out = fopen("/Users/corrado/CLionProjects/csv_to_arrow/myfile.arrow", "w");


        /*
    std::shared_ptr<arrow::io::OutputStream> os;
    arrow::io::OutputStream *outstream;
    //os->
    std::shared_ptr<arrow::io::FileOutputStream> out;
    //out->Open("/Users/corrado/CLionProjects/csv_to_arrow/myfile.arrow", os);
    arrow::Status status;
    //status = arrow::io::FileOutputStream::Open("/Users/corrado/CLionProjects/csv_to_arrow/myfile.arrow", &os);

    std::shared_ptr<arrow::Field> f1 = arrow::field("a",arrow::int8());
    std::shared_ptr<arrow::Field> f2 = arrow::field("b",arrow::int8());
    std::shared_ptr<arrow::Field> f3 = arrow::field("c",arrow::int8());
    std::shared_ptr<arrow::Field> f4 = arrow::field("d",arrow::int8());
    std::shared_ptr<arrow::Field> f5 = arrow::field("e",arrow::int8());
    std::shared_ptr<arrow::Field> f6 = arrow::field("f",arrow::int8());
    std::shared_ptr<arrow::Field> f7 = arrow::field("g",arrow::int8());
    std::shared_ptr<arrow::Field> f8 = arrow::field("h",arrow::int8());

    std::shared_ptr<arrow::Schema> schema = arrow::schema({f1,f2,f3,f4,f5,f6,f7,f8});

    */
    //std::cout << "open file : " << status.ok() << std::endl;
    //auto rbw = arrow::ipc::RecordBatchFileWriter::Open(outstream,schema);
    //std::cout<<rbw.ok()<<std::endl;
    //arrow::dataset::CsvFileFormat::

    /*
    std::shared_ptr<arrow::io::OutputStream> out;
    out->
    arrow::ipc::RecordBatchFileWriter::
    arrow::io::WritableFile::
    */
}





// Do I need to pass in a memory pool as a parameter?
std::shared_ptr<arrow::Table> build_table_old(const std::string& file_path, arrow::MemoryPool *pool, std::vector<std::string> schema) {//, std::shared_ptr<arrow::Table> *table) {

    arrow::Status status;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    status = arrow::io::ReadableFile::Open(file_path, pool, &infile);

    if(!status.ok()) {
        std::cout << "Error reading file." << std::endl;
    }

    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.column_names = schema;
    //read_options.block_size = 1024;
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

    if(!status.ok()) {
        std::cout << "Error reading csv into table." << std::endl;
    }

    return table;
    //std::cout<<"customer->num_rows() = " << table->num_rows() << std::endl;
}

int main2() {

    std::shared_ptr<arrow::io::InputStream> instream;

    //std::shared_ptr<arrow::io::ReadableFile> infile;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    // For whatever reason, Open() could not find the file unless I provided an absolute path. Just use absolute paths
    // from now on.
    //arrow::Status st = arrow::io::ReadableFile::Open("/Users/corrado/CLionProjects/csv_to_arrow/test.csv", arrow::default_memory_pool(), &infile);

    arrow::Status st = arrow::io::ReadableFile::Open("/Users/corrado/SQL-benchmark-data-generator/ssbgen/supplier.tbl", arrow::default_memory_pool(), &infile);
    std::cout<<"Open file ok? : " << st.ok() << std::endl;

    //std::shared_ptr<arrow::Buffer> buffer;
    //st = infile->Read(3, &buffer);

    //cout << "Read ok? : " << st.ok() << endl;
    //cout << "Buffer contents? : " << buffer->ToString() << endl;


    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.block_size = 1024;
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    std::shared_ptr<arrow::csv::TableReader> reader;
    st = arrow::csv::TableReader::Make(arrow::default_memory_pool(), infile, read_options, parse_options, convert_options, &reader);

    std::cout << "TableReader ok? : "<< st.ok() << std::endl;

    std::shared_ptr<arrow::Field> f1 = arrow::field("a",arrow::int8());
    std::shared_ptr<arrow::Field> f2 = arrow::field("b",arrow::int8());
    std::shared_ptr<arrow::Field> f3 = arrow::field("c",arrow::int8());
    std::shared_ptr<arrow::Field> f4 = arrow::field("d",arrow::int8());
    std::shared_ptr<arrow::Field> f5 = arrow::field("e",arrow::int8());

    //arrow::Table::Make(arrow::schema({f1,f2,f3,f4,f5}));

    std::shared_ptr<arrow::Table> table;
    // Ah! This returns an error if we have multiple things reading the file at once! I had to comment out my initial
    // read test to get this to work!
    st = reader->Read(&table);


    std::cout <<"Read table ok? : "<< st.ok() << std::endl;
    std::cout<<"table->num_columns() = " << table->num_columns() << std::endl;
    std::cout<<"table->num_rows() = " << table->num_rows() << std::endl;
    std::cout<<"table->column(0)->num_chunks() = " << table->column(0)->num_chunks() << std::endl;

    /*
    for (int i=0; i<table->num_rows(); i++) {
        std::cout << table->column(i)->chunk(0)->ToString() << std::endl;
    }
*/

    // When we say "arrow file", what exactly do we mean by that?
    /*
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    arrow::io::FileOutputStream::Open("/Users/corrado/output.arrow", &outfile);
    const std::shared_ptr<arrow::Schema> schema = table->schema();

    arrow::ipc::RecordBatchFileWriter::Open(outfile,schema);
    */
}


int main4() {

    std::shared_ptr<arrow::io::ReadableFile> infile;
    arrow::Status st = arrow::io::ReadableFile::Open("/Users/corrado/CLionProjects/csv_to_arrow/lineorder.tbl", arrow::default_memory_pool(), &infile);
    //arrow::Status st = arrow::io::ReadableFile::Open("/Users/corrado/CLionProjects/csv_to_arrow/test.csv", arrow::default_memory_pool(), &infile);
    std::cout<<"Open file ok? : " << st.ok() << std::endl;

    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.block_size = 64;
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = ',';
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::shared_ptr<arrow::csv::TableReader> reader;
    // TableReader has no problem with the 60GB file...
    //st = arrow::csv::TableReader::Make(arrow::default_memory_pool(), infile, read_options, parse_options, convert_options, &reader);


    // DON'T CONFUSE TABLEREADER OK MESSAGE WITH READ TABLE OK MESSAGE!
    //std::cout << "TableReader ok? : "<< st.ok() << std::endl;

    std::shared_ptr<arrow::Table> table;
    // ...but here, we get issues because we're actually reading the csv file into a table
    //st = reader->Read(&table);

    //arrow::ipc::DictionaryMemo tmp = arrow::ipc::DictionaryMemo();

    std::shared_ptr<arrow::Field> f1 = arrow::field("a",arrow::int8());
    std::shared_ptr<arrow::Field> f2 = arrow::field("b",arrow::int8());
    std::shared_ptr<arrow::Field> f3 = arrow::field("c",arrow::int8());
    std::shared_ptr<arrow::Field> f4 = arrow::field("d",arrow::int8());
    std::shared_ptr<arrow::Field> f5 = arrow::field("e",arrow::int8());

    std::shared_ptr<arrow::Schema> s = arrow::schema({f1,f2,f3,f4,f5});
    std::shared_ptr<arrow::RecordBatch> batch;

    std::cout << "TEST" << std::endl;
    //arrow::ipc::ReadRecordBatch(s,&tmp,infile,batch);
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> batch_reader;

    //auto f = std::static_pointer_cast<arrow::io::RandomAccessFile>(infile);
    std::cout << "TEST" << std::endl;
    st = arrow::ipc::RecordBatchFileReader::Open(infile, &batch_reader);
    std::cout << "TEST" << std::endl;
    //std::cout<< "batch_reader->num_record_batches = " << batch_reader->num_record_batches() << std::endl;

    std::cout << "TEST"<< st.ok() << std::endl;

    //std::cout <<"Read table ok? : "<< st.ok() << std::endl;
    //std::cout<<"table->num_columns() = " << table->num_columns() << std::endl;
    //std::cout<<"table->num_rows() = " << table->num_rows() << std::endl;
    //std::cout<<"table->column(0)->num_chunks() = " << table->column(0)->num_chunks() << std::endl;

    //std::shared_ptr<arrow::ipc::RecordBatchFileReader> batch_reader;
    //st = arrow::ipc::RecordBatchFileReader::Open(infile, &batch_reader);

    //std::cout <<"RecordBatchFileReader ok? : "<< st.ok() << std::endl;

    //std::shared_ptr<arrow::RecordBatch> batch;
    //st = batch_reader->ReadRecordBatch(0, &batch);

    //std::cout <<"RecordBatch ok? : "<< st.ok() << std::endl;

    //std::cout<<"table->num_columns() = " << table->num_rows() << std::endl;
    //std::cout<<"table->column(0)->num_chunks() = " << table->column(0)->num_chunks() << std::endl;

    /*
    for (int i=0; i<table->num_rows(); i++) {
        std::cout << table->column(i)->chunk(0)->ToString() << std::endl;
    }
*/
}

void read_lineorder() {

    std::shared_ptr<arrow::io::ReadableFile> infile;
    arrow::Status st = arrow::io::ReadableFile::Open("/Users/corrado/SQL-benchmark-data-generator/ssbgen/lineorder.tbl", arrow::default_memory_pool(), &infile);
    std::cout<<"Open file ok? : " << st.ok() << std::endl;

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::shared_ptr<arrow::csv::TableReader> reader;
    // TableReader has no problem with the 60GB file...
    st = arrow::csv::TableReader::Make(arrow::default_memory_pool(), infile, read_options, parse_options, convert_options, &reader);


    // DON'T CONFUSE TABLEREADER OK MESSAGE WITH READ TABLE OK MESSAGE!
    std::cout << "TableReader ok? : "<< st.ok() << std::endl;

    std::shared_ptr<arrow::Table> table;
    // ...but here, we get issues because we're actually reading the csv file into a table
    //st = reader->Read(&table);

    std::cout <<"Read table ok? : "<< st.ok() << std::endl;


    //std::shared_ptr<arrow::ipc::RecordBatchFileReader> batch_reader;
    //st = arrow::ipc::RecordBatchFileReader::Open(infile, &batch_reader);

    std::cout <<"RecordBatchFileReader ok? : "<< st.ok() << std::endl;

    //std::shared_ptr<arrow::RecordBatch> batch;
    //st = batch_reader->ReadRecordBatch(0, &batch);

    std::cout <<"RecordBatch ok? : "<< st.ok() << std::endl;

    //std::cout<<"table->num_columns() = " << table->num_rows() << std::endl;
    //std::cout<<"table->column(0)->num_chunks() = " << table->column(0)->num_chunks() << std::endl;

    /*
    for (int i=0; i<table->num_rows(); i++) {
        std::cout << table->column(i)->chunk(0)->ToString() << std::endl;
    }
*/
}


int main_old() {

    arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::InputStream> input;
    std::shared_ptr<arrow::io::ReadableFile> file;

    if (!(arrow::io::ReadableFile::Open("test2.csv", &file)).ok()){
        std::cout<<"AH"<<std::endl;
    }
    std::shared_ptr<arrow::Table> t;
    std::shared_ptr<arrow::csv::TableReader> r;
    //r->Read(t);
    int64_t bytes_read = 42;
    uint8_t buffer[100];
    //file->Read(3, &bytes_read, buffer);
    std::cout<<buffer[0]<<" "<<bytes_read<<std::endl;
    //std::shared_ptr<arrow::io::ReadableFile> f;
    //arrow::Status status = arrow::io::ReadableFile::Open(0, &f);
    //std::cout << status.ok() << std::endl;
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    std::shared_ptr<arrow::csv::TableReader> reader;
    st = arrow::csv::TableReader::Make(pool, file, read_options, parse_options, convert_options, &reader);
    //std::cout << st.ok() << std::endl;
    std::shared_ptr<arrow::Table> table;
    st = reader->Read(&table);
    std::cout << st.ok() << std::endl;

    std::shared_ptr<arrow::Buffer> buf;
    st = file->Read(3, &buf);
    std::cout<< st.ok() << buf->ToString() << std::endl;


    std::cout << table.use_count() << std::endl;
    for (int c=0; c<2; c++) {
        std::cout<<"c = " << c << std::endl;
        auto col = table->column(c);
        std::cout<<"name: " << col << std::endl;
    }
    //int x = table->num_rows();
    //std::cout << table->num_rows() <<std::endl;

    //if (!st.ok()) {
    // Handle TableReader instantiation error...
       // std::cout<< "table reader error" << std::endl;
    //}

    //std::shared_ptr<arrow::Table> table;
    // Read table from CSV file
    //st = reader->Read(&table);
    //if (!st.ok()) {
    // Handle CSV read error
    // (for example a CSV syntax error or failed type conversion)
    //}

 }