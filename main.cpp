//
// Created by Nicholas Corrado on 10/29/19.
//
#include "Queries.h"

int main(int argc, char *argv[]){

	if (argc <= 1){
		ui();
	}
	else if (argc >= 3){
		run(argv[1], argv[2]);
	}
	else{
		std::cout << "Invalid arguments." << std::endl;
	}
    return 0;
}
