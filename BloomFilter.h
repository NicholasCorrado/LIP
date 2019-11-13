#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

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
	
	std::string foreign_key;
	int count;
	int pass;

public:
	BloomFilter();
	BloomFilter(std::vector<long long> elements);
	BloomFilter(std::vector<std::string> elements);
	
	double GetFilterRate();
	void IncrementCount();
	void IncrementPass();
	void Reset();

	void SetForeignKey(std::string fk);
	std::string GetForeignKey();
	
	void Insert(long long value);
	void Insert(std::string value);

	bool Search(long long value);
	bool Search(std::string value);
};

#endif
