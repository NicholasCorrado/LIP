//
// Created by Nicholas Corrado on 10/29/19.
//
#include "Queries.h"
#include "util/BloomFilterTest.h"
#include <chrono>

int main(int argc, char *argv[]){

	if (argc <= 1){
		ui();
	}
	else if (argc==2) {
        ProbeTest(argv[1]);
	}
	else if (argc == 4){
    // ./csv_to_arrow [4.2] [lip/hash] [SF]
        run(argv[1], argv[2], argv[3], false);
	}
	else if (argc == 5){
            bool enum_flag = (strcmp(argv[3], "y") == 0 || strcpy(argv[3], "Y") == 0);
            run(argv[1], argv[2], argv[3], enum_flag);
	}
	else{
		std::cout << "Invalid arguments." << std::endl;
	}

    return 0;
}
