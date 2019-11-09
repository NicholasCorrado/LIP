#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>
#include "hash.h"

class BloomFilter{

private:
	int numberOfCells;
	int numberOfHashes;
	bool* cells;
	int* seeds;
	

	int count = 0;
	int pass = 0;

public:
	double getFilterRate();
	void incrementCount();
	void incrementPass();

	BloomFilter(std::vector<int> elements);
	BloomFilter(std::vector<std::string> elements);


	void insert(const void* value);
	void insert(std::string value);

	bool search(int value);
	bool search(std::string value);
};
