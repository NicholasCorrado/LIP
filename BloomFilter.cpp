#include<iostream>
#include<stdlib.h>
#include<string.h>
#include<map>
#include<math.h>
#include<vector>
#include<algorithm>
#include <chrono>
#include <functional>
#include "BloomFilter.h"
#include "util/sparsepp/spp.h"


#define FALSE_POSITIVE_RATE 0.001
#define MAX_SEED 65535
#define S 4
#define DEFAULT_INSERT 500000

/*
	
	HASH BELOW

	We use Murmurhash.

	The following code is accessible at 
		https://github.com/aappleby/smhasher/tree/master/src

*/



#define FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32 ( uint32_t x, int8_t r )
{
  return (x << r) | (x >> (32 - r));
}

inline uint64_t rotl64 ( uint64_t x, int8_t r )
{
  return (x << r) | (x >> (64 - r));
}

#define ROTL32(x,y) rotl32(x,y)
#define ROTL64(x,y) rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)


//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

FORCE_INLINE uint32_t getblock32 ( const uint32_t * p, int i )
{
  return p[i];
}

FORCE_INLINE uint64_t getblock64 ( const uint64_t * p, int i )
{
  return p[i];
}

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

FORCE_INLINE uint32_t fmix32 ( uint32_t h )
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

//----------

FORCE_INLINE uint64_t fmix64 ( uint64_t k )
{
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;

  return k;
}

//-----------------------------------------------------------------------------

void MurmurHash3_x86_32 ( const void * key, int len,
                          uint32_t seed, void * out )
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 4;

  uint32_t h1 = seed;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  //----------
  // body

  const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);

  for(int i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock32(blocks,i);

    k1 *= c1;
    k1 = ROTL32(k1,15);
    k1 *= c2;
    
    h1 ^= k1;
    h1 = ROTL32(h1,13); 
    h1 = h1*5+0xe6546b64;
  }

  //----------
  // tail

  const uint8_t * tail = (const uint8_t*)(data + nblocks*4);

  uint32_t k1 = 0;

  switch(len & 3)
  {
  case 3: k1 ^= tail[2] << 16;
  case 2: k1 ^= tail[1] << 8;
  case 1: k1 ^= tail[0];
          k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len;

  h1 = fmix32(h1);

  *(uint32_t*)out = h1;
} 




/*
	Function to hash an integer, with a seed.
	returns a hash.
*/


unsigned int MurmurHash(const void* x, int seed){
	int len = 4;
	unsigned int ret;
	MurmurHash3_x86_32 (x, len, seed, &ret );
	return ret;
}


/*
	Function to hash a string, with a seed.
	returns a hash.
*/

unsigned int MurmurHashStr(const std::string x, int seed){
	int len = x.length();

	unsigned int ret;

	char* tmp = (char*)malloc(len * sizeof(char));

	strcpy(tmp, x.c_str());

	MurmurHash3_x86_32 (tmp, len, seed, &ret );
	
	free(tmp);
	return ret;

}


unsigned int hash_function(long long x, int seed) {
	x = (x<<32)^seed;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}


unsigned int Hash(long long x, int seed){
	return hash_function(x, seed);
}

unsigned HashStr(const std::string x, int seed){
	return MurmurHashStr(x, seed);
}


/* 

	BLOOM FILTERS ARE BELOW

*/




/*
	Return the current filter rate = pass/count;
*/
double BloomFilter::GetFilterRate(){
	if (count > 0) 
		return 1.0 * pass / count;
	else
		return 1;
}


/*
	Increment the count variable
*/
void BloomFilter::IncrementCount(){
	count++;
}


/*
	Increment the pass variable
*/
void BloomFilter::IncrementPass(){
	pass++;
}


void BloomFilter::SetForeignKey(std::string fk){
	foreign_key = fk;
}

std::string BloomFilter::GetForeignKey(){
	return foreign_key;
}
	




/*
	Constructor to initialize an empty Bloomfilter to be inserted.

	We assume by default the number of elements to be inserted is 500000.
*/
BloomFilter::BloomFilter(){
	Reset();
	int n = DEFAULT_INSERT;
	numberOfHashes = int ( - log(FALSE_POSITIVE_RATE) / log(2));
	numberOfCells = int(n * numberOfHashes / log(2));

	cells = std::vector<bool>(numberOfCells, false);

	seeds = (int*)malloc(numberOfHashes * sizeof(int));
	for(int i = 0; i < numberOfHashes; ++i){
		seeds[i] = rand() % MAX_SEED;
	}
}

/*
	Constructor to initialize an empty Bloomfilter to be inserted.

	The user can pass in a how many inserts will be performed.
*/
BloomFilter::BloomFilter(int num_insert, int k){
    Reset();
    int n = num_insert;
    //numberOfHashes = int ( - log(FALSE_POSITIVE_RATE) / log(2));
    numberOfHashes = int (round( - log(FALSE_POSITIVE_RATE) / log(2)));
    numberOfCells = int(n * numberOfHashes / log(2));

    cells = std::vector<bool>(numberOfCells, false);

    seeds = (int*)malloc(numberOfHashes * sizeof(int));
    for(int i = 0; i < numberOfHashes; ++i){
        seeds[i] = rand() % MAX_SEED;
    }

    memory_k = k;
    time = 0;

    for (int i=0; i<memory_k; i++) {
        pass_queue.push(0);
        count_queue.push(0);
    }
}


/*
	Constructor to initialize a Bloomfilter for a vector of integers.
	Number of cells and hash functions are calculated from the false 
	positve rate.
*/
BloomFilter::BloomFilter(std::vector<long long> elements){
	Reset();
	int n = elements.size();
	numberOfHashes = int ( - log(FALSE_POSITIVE_RATE) / log(2));
	numberOfCells = int(n * numberOfHashes / log(2));

	cells = std::vector<bool>(numberOfCells, false);

	seeds = (int*)malloc(numberOfHashes * sizeof(int));
	for(int i = 0; i < numberOfHashes; ++i){
		seeds[i] = rand() % MAX_SEED;
	}


	for(int i = 0; i < n; ++i){
		Insert(elements[i]);
	}
}



/*
	Constructor to initialize a Bloomfilter for a vector of string.
	Number of cells and hash functions are calculated from the false 
	positve rate.
*/
BloomFilter::BloomFilter(std::vector<std::string> elements){
	Reset();
	int n = elements.size();
	numberOfHashes = int ( - log(FALSE_POSITIVE_RATE) / log(2));
	numberOfCells = int(n * numberOfHashes / log(2));

	cells = std::vector<bool>(numberOfCells, false);

	seeds = (int*)malloc(numberOfHashes * sizeof(int));
	for(int i = 0; i < numberOfHashes; ++i){
		seeds[i] = rand() % MAX_SEED;
	}


	for(int i = 0; i < n; ++i){
		Insert(elements[i]);
	}
}



/*
	Reset the count and pass counter

*/

void BloomFilter::Reset(){
	pass = 0;
	count = 0;
	pass_queue_sum = 0;
	count_queue_sum = 0;
}


/*
	Insert an integer to the bloom filter
*/
void BloomFilter::Insert(long long value){
	for(int i = 0; i < numberOfHashes; ++i){
		int index = Hash(value, seeds[i]) % numberOfCells;
		cells[index] = true;
	}
}

/*
	Insert a string to the Bloom filter
*/
void BloomFilter::Insert(std::string value){
	for(int i = 0; i < numberOfHashes; ++i){
		int index = HashStr(value, seeds[i]) % numberOfCells;
		cells[index] = true;
	}
}


/*
	Search for a int value in the bloom filter and return true if found,
	false otherwise.	
*/
bool BloomFilter::Search(long long value){

	for(int i = 0; i < numberOfHashes; ++i){
		int index = Hash(value, seeds[i]) % numberOfCells;
		if (!cells[index]) return false;
	}

	return true;
}


/*
	Search for a string in the bloom filter and return true if found,
	false otherwise.	
*/
bool BloomFilter::Search(std::string value){
	for(int i = 0; i < numberOfHashes; ++i){
		int index = HashStr(value, seeds[i]) % numberOfCells;
		if (!cells[index]) return false;
	}
	return true;
}


void BloomFilter::SetMemory(int _memory){
	memory_k = _memory;
}


void BloomFilter::BatchEndUpdate(){

    pass_queue_sum  -= pass_queue.front();
    count_queue_sum -= count_queue.front();

    pass_queue.pop();
    count_queue.pop();

	pass_queue.push(pass);
    count_queue.push(count);

	pass_queue_sum += pass;
	count_queue_sum += count;

    pass = 0;
	count = 0;
}

double BloomFilter::GetFilterRateK() {

    if (count_queue_sum > 0)
        return 1.0 * pass_queue_sum / count_queue_sum;
    else
        return 1;
}

double BloomFilter::GetFilterRateTime() {
    if (count > 0) {
        return 1.0 * time/count;
    }
    else {
        return 1;
    }
}

/*
	Sample test program to test the false positive rate.
*/

int test_false_positive(){

	std::vector<long long> integers;
	std::map<long long, bool> m;
	
	int input_size = 10000;
	int test_sequence_size = 1000000;


	for(int i = 0; i < input_size; ++i){
		long long val = rand() % MAX_SEED;
		m[val] = true;
		integers.push_back(val);
	}

	BloomFilter* bf = new BloomFilter(integers);

	int cnt = 0;
	int fp = 0;
	for(int i = 0; i < test_sequence_size; ++i){
		long long test = rand() % MAX_SEED;
		cnt ++;

		if (m.count(test) == 0){
			cnt ++;
			if (bf->Search(test)){
				fp++;
			}
		}

	}
	std::cout << fp << " " << cnt<< std::endl;
	std::cout << 1.0 * (fp) / cnt << std::endl;
	return 0;
}




/*
	Compare function comparing the filter rate.
*/
bool BloomFilterCompare( BloomFilter *lhs,  BloomFilter *rhs)
{
  return lhs -> GetFilterRate() < rhs -> GetFilterRate();
}

bool BloomFilterCompareK( BloomFilter *lhs,  BloomFilter *rhs)
{
    return lhs -> GetFilterRateK() < rhs -> GetFilterRateK();
}

bool BloomFilterCompareTime( BloomFilter *lhs,  BloomFilter *rhs)
{
    return lhs -> GetFilterRateTime() < rhs -> GetFilterRateTime();
}



/*
	Sample test program to test adaptive filters.
*/
void sort_filters(){
	std::vector<long long> integers;
	
	int input_size = S;	


	BloomFilter* bf[S];
	for(int i = 0; i < S; ++i){
		for(int i = 0; i < input_size; ++i){
			long long val = rand() % MAX_SEED;
			integers.push_back(val);
		}
		bf[i] = new BloomFilter(integers);
		bf[i] -> SetMemory(3);
	}


	int test_size = 10;

	int block = 2;

	for(int round = 0; round < test_size / block; ++round){
		for(int j = 0; j < block; ++j){
			long long test = rand() % MAX_SEED;

			for(int i = 0; i < S; ++i){
				bf[i] -> IncrementCount();
				if (bf[i] -> Search(test)){
					bf[i] -> IncrementPass();
				}
			}
		}
		
		std::sort(bf, bf + S, BloomFilterCompare);
		std::cout << "Round #" << round << std::endl;
		for(int i = 0; i < S; ++i){
			std::cout << bf[i] -> GetFilterRate() <<  " " ;
		}
		std::cout << std::endl;
	}
	
}



/*
	Generate a random string of a specified length
	Input: 
		len - the desired length of the random string
	Output:
		a random string of the specified length
*/
std::string gen_random_str(const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string ret = "";


    for (int i = 0; i < len; ++i) {
        ret = ret + alphanum[rand() % (sizeof(alphanum) - 1)];
    }

  	return ret;
}




/*
	Sample test program to test the random string false positive
	rate.
*/
int test_random_string(){

	std::vector<std::string> strings;
	std::map<std::string, bool> m;
	
	int input_size = 10000;
	int test_sequence_size = 10000000;

	int length = 100;

	for(int i = 0; i < input_size; ++i){
		std::string val = gen_random_str(length);
		m[val] = true;
		//cout << val << endl;
		strings.push_back(val);
	}

	BloomFilter* bf = new BloomFilter(strings);

	int cnt = 0;
	int fp = 0;

	for(int i = 0; i < test_sequence_size; ++i){
		if (i % (test_sequence_size / 100) == 0){
			std::cout << 100 * i / test_sequence_size << "%" << std::endl;
		}
		std::string test = gen_random_str(length);
		//cout << test << endl;
		int res = m.count(test);
		if (res == 0){
			cnt ++;
			bool res2 = bf -> Search(test);
			if (res2){
				fp++;
			}
		}
	}
	std::cout << fp << " " << cnt<< std::endl;
	std::cout << 1.0 * (fp) / cnt << std::endl;

	return 0;
}

int local_main(){
	test_false_positive();
	//sort_filters();
	test_random_string();
	return 0;

}