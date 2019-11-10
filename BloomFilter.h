#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>

class BloomFilter{

private:
	int numberOfCells;
	int numberOfHashes;
	bool* cells;
	int* seeds;
	

	int count;
	int pass;

public:
	double getFilterRate();
	void incrementCount();
	void incrementPass();

	BloomFilter();
	BloomFilter(std::vector<int> elements);
	BloomFilter(std::vector<std::string> elements);


	void insert(int value);
	void insert(std::string value);

	bool search(int value);
	bool search(std::string value);
};
