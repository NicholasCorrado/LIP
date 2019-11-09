#include<iostream>
#include<stdlib.h>
#include<string.h>
#include<map>
#include<math.h>
#include<vector>
#include<algorithm>

#define MAX_SEED 65535
#define FALSE_POSITIVE_RATE 0.001
#define S 4


using namespace std;



#define	FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32 ( uint32_t x, int8_t r )
{
  return (x << r) | (x >> (32 - r));
}

inline uint64_t rotl64 ( uint64_t x, int8_t r )
{
  return (x << r) | (x >> (64 - r));
}

#define	ROTL32(x,y)	rotl32(x,y)
#define ROTL64(x,y)	rotl64(x,y)

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


unsigned int MurmurHash(const void* x, int seed) {
	unsigned int ret;
	MurmurHash3_x86_32 (x, 4, seed, &ret);
	return ret;
}





class BloomFilter{

private:
	int numberOfCells;
	int numberOfHashes;
	bool* cells;
	int* seeds;
	

	int count = 0;
	int pass = 0;

public:

	

	double getFilterRate(){
		if (count > 0) 
			return 1.0 * pass / count;
		else
			return 1;
	}

	void incrementCount(){
		count++;
	}

	void incrementPass(){
		pass++;
	}

	BloomFilter(vector<int> elements){
		int n = elements.size();
		numberOfHashes = int ( - log(FALSE_POSITIVE_RATE) / log(2));
		numberOfCells = int(n * numberOfHashes / log(2));

		cells = (bool*)malloc(numberOfCells * sizeof(bool));
		for(int i = 0; i < numberOfCells; ++i){
			cells[i] = false;
		}

		seeds = (int*)malloc(numberOfHashes * sizeof(int));
		for(int i = 0; i < numberOfHashes; ++i){
			seeds[i] = rand() % MAX_SEED;
		}


		for(int i = 0; i < n; ++i){
			insert(&elements[i]);
		}
	}


	BloomFilter(vector<string> elements){
		int n = elements.size();
		numberOfHashes = int ( - log(FALSE_POSITIVE_RATE) / log(2));
		numberOfCells = int(n * numberOfHashes / log(2));

		cells = (bool*)malloc(numberOfCells * sizeof(bool));
		for(int i = 0; i < numberOfCells; ++i){
			cells[i] = false;
		}

		seeds = (int*)malloc(numberOfHashes * sizeof(int));
		for(int i = 0; i < numberOfHashes; ++i){
			seeds[i] = rand() % MAX_SEED;
		}


		for(int i = 0; i < n; ++i){
			insert(&elements[i]);
		}
	}


	void insert(const void* value){
		for(int i = 0; i < numberOfHashes; ++i){
			int index = MurmurHash(value, seeds[i]) % numberOfCells;
			cells[index] = true;
		}
	}

	bool search(const void* value){
		for(int i = 0; i < numberOfHashes; ++i){
			int index = MurmurHash(value, seeds[i]) % numberOfCells;
			if (!cells[index]) return false;
		}
		return true;
	}
};



int test_false_positive(){

	vector<int> integers;
	map<int, bool> m;
	
	int input_size = 10000;
	int test_sequence_size = 1000000;


	for(int i = 0; i < input_size; ++i){
		int val = rand() % MAX_SEED;
		m[val] = true;
		integers.push_back(val);
	}

	BloomFilter* bf = new BloomFilter(integers);

	int cnt = 0;
	int fp = 0;
	for(int i = 0; i < test_sequence_size; ++i){
		int test = rand() % MAX_SEED;
		cnt ++;

		if (m.count(test) == 0){
			cnt ++;
			if (bf->search(&test)){
				fp++;
			}
		}

	}
	cout << fp << " " << cnt<< endl;
	cout << 1.0 * (fp) / cnt << endl;
	return 0;
}


bool comp( BloomFilter *lhs,  BloomFilter *rhs)
{
  return lhs -> getFilterRate() < rhs -> getFilterRate();
}


void sort_filters(){
	vector<int> integers;
	
	int input_size = S;	


	BloomFilter* bf[S];
	for(int i = 0; i < S; ++i){
		cout << i << endl;
		for(int i = 0; i < input_size; ++i){
			int val = rand() % MAX_SEED;
			integers.push_back(val);
		}
		bf[i] = new BloomFilter(integers);
	}


	int test_size = 10;

	int block = 2;

	for(int round = 0; round < test_size / block; ++round){
		for(int j = 0; j < block; ++j){
			int test = rand() % MAX_SEED;

			for(int i = 0; i < S; ++i){
				bf[i] -> incrementCount();
				if (bf[i] -> search(&test)){
					bf[i] -> incrementPass();
				}
			}
		}
		
		sort(bf, bf + S, comp);
		cout << "Round #" << round << endl;
		for(int i = 0; i < S; ++i){
			cout << bf[i] -> getFilterRate() <<  " " ;
		}
		cout << endl;
	}
	
}



int main(){

	//test_false_positive();
	sort_filters();
	return 0;

}