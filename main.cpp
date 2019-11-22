//
// Created by Nicholas Corrado on 10/29/19.
//
#include "Queries.h"

int main(int argc, char *argv[]){

	if (argc <= 1){
		ui();
	}
	else if (argc == 3){
		run(argv[1], argv[2], false);
	}
	else if (argc >= 4){
		bool enum_flag = (argv[3] == "y" || argv[3] == "Y");
		run(argv[1], argv[2], argv[3]);
	}
	else{
		std::cout << "Invalid arguments." << std::endl;
	}



    return 0;
}
