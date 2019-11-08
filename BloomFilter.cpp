#include<iostream>
#include<stdlib.h>
#include<string.h>
#include<map>
#include<math.h>
#include<vector>

#define A 54059 /* a prime */
#define B 76963 /* another prime */
#define C 86969 /* yet another prime */
#define FIRSTH 37 /* also prime */
#define MAX_SEED 65535



using namespace std;


unsigned int hash_int(unsigned int x, int seed) {
	x = x + seed;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}


unsigned int hash_str(string s, int seed)
{
   unsigned h = FIRSTH;
   for(int i = 0; i < s.length(); ++i){
   	h = (h * A) ^ ((s[i] + seed) * B);
   }
   return h; // or return h % C;
}



class BloomFilter{

private:
	int numberOfCells;
	int numberOfHashs;
	bool* cells;
	int* seeds;

public:

	BloomFilter(vector<int> integers){
		int n = integers.size();
		int numberOfCells = 100000;
		int numberOfHashs = log(2) * numberOfCells / n;

		cells = (bool*)malloc(numberOfCells * sizeof(bool));
		for(int i = 0; i < numberOfCells; ++i){
			cells[i] = false;
		}

		seeds = (int*)malloc(numberOfHashs * sizeof(int));
		for(int i = 0; i < numberOfHashs; ++i){
			seeds[i] = rand() % MAX_SEED;
		}


		for(int i = 0; i < n; ++i){
			insert(integers[i]);
		}
	}





	BloomFilter(int numOfCells, int numOfHashes){
		numberOfCells = numOfCells;
		cells = (bool*)malloc(numberOfCells * sizeof(bool));
		for(int i = 0; i < numberOfCells; ++i){
			cells[i] = false;
		}

		numberOfHashs = numOfHashes;
		seeds = (int*)malloc(numberOfHashs * sizeof(int));
		for(int i = 0; i < numberOfHashs; ++i){
			seeds[i] = rand() % MAX_SEED;
		}

	}

	void insert(int value){
		for(int i = 0; i < numberOfHashs; ++i){
			int index = hash_int(value, seeds[i]) % numberOfCells;
			cells[index] = true;
		}
	}

	void insert(string value){
		for(int i = 0; i < numberOfHashs; ++i){
			int index = hash_str(value, seeds[i]) % numberOfCells;
			cells[index] = true;
		}
	}


	bool search(int value){
		for(int i = 0; i < numberOfHashs; ++i){
			int index = hash_int(value, seeds[i]) % numberOfCells;
			if (!cells[index]) return false;
		}
		return true;
	}

	bool search(string value){
		for(int i = 0; i < numberOfHashs; ++i){
			int index = hash_str(value, seeds[i]) % numberOfCells;
			if (!cells[index]) return false;
		}
		return true;
	}
};


int main1(){

	BloomFilter* bf = new BloomFilter(1000000, 6);

	map<int, bool> m;

	for(int i = 0; i < 10000; ++i){
		int val = rand() % MAX_SEED;
		bf -> insert(val);
		m[val] = true;
	}
	int cnt = 0;
	int err = 0;
	for(int i = 0; i < 1000000; ++i){
		int test = rand() % MAX_SEED;
		if (!m[test] && bf -> search(test)){
			err++;
		}
		cnt++;
	}
	cout << 1.0 * (cnt - err) / cnt << endl;
	return 0;
}

int main(){

	vector<int> integers;
	map<int, bool> m;

	for(int i = 0; i < 100000; ++i){
		int val = rand() % MAX_SEED;
		m[val] = true;
		integers.push_back(val);
	}

	BloomFilter* bf = new BloomFilter(integers);

	int cnt = 0;
	int err = 0;
	for(int i = 0; i < 1000000; ++i){
		int test = rand() % MAX_SEED;
		if (!m[test] && bf -> search(test)){
			err++;
		}
		cnt++;
	}
	cout << 1.0 * (cnt - err) / cnt << endl;
	return 0;
}
