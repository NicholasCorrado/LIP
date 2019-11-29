#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>
#include<queue>

class BloomFilter{

private:
    int numberOfCells;
	int numberOfHashes;
	std::vector<bool> cells;
	int* seeds;
	
	std::string foreign_key;
	int count;
	int pass;


	int pass_queue_sum;
	int count_queue_sum;

	int memory_k;
	std::queue<int> pass_queue;
	std::queue<int> count_queue;

public:

    ///int numberOfCells;
	BloomFilter();
    BloomFilter(int num_insert);
	BloomFilter(std::vector<long long> elements);
	BloomFilter(std::vector<std::string> elements);
    BloomFilter(int num_insert, int num_cells);
	
	double GetFilterRate();
    double GetFilterRateK();
	void IncrementCount();
	void IncrementPass();
	void Reset();

	void SetForeignKey(std::string fk);
	std::string GetForeignKey();
	
	void Insert(long long value);
	void Insert(std::string value);

	bool Search(long long value);
	bool Search(std::string value);

	void SetMemory(int _memory);
	void BatchEndUpdate();
};

void TestTrueNegative();
void CostOfHashTableProbe();
void CostOfBloomFilterProbe();

#endif
