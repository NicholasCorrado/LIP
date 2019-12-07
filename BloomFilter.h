#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include<iostream>
#include<stdlib.h>
#include<vector>
#include<string>
#include<queue>
#include <chrono>

class BloomFilter{

private:
    int numberOfCells;
	int numberOfHashes;
	std::vector<bool> cells;
	int* seeds;
	
	std::string foreign_key;
	//int count;
	int pass;



	long long pass_queue_sum;
	long long count_queue_sum;

	int memory_k;
//	std::queue<int> pass_queue;
//	std::queue<int> count_queue;

    int* pass_queue;
    int* count_queue;

//    std::unique_ptr<long*> pass_queue;
//    std::unique_ptr<long*> count_queue;
    int queue_index;

public:
    int count;
    unsigned long long time;

	BloomFilter();
    BloomFilter(int num_insert);
	BloomFilter(std::vector<long long> elements);
	BloomFilter(std::vector<std::string> elements);

	double GetFilterRate();
    double GetFilterRateK();
    double GetFilterRateTime();
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
