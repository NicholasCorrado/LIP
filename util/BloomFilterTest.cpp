//
// Created by Nicholas Corrado on 11/23/19.
//

#include "BloomFilterTest.h"
#include <iostream>
#include "./../BloomFilter.h"
#include "sparsepp/spp.h"
#include <chrono>
void TestTrueNegative() {

    int num_probes = 1000000;
    int num_insert = 60000;

    for (int num_cells=1000; num_cells<1000000; num_cells = num_cells + 1000) {
        //for (int num_insert = 100; num_insert< 2*num_cells; num_insert = num_insert + 100) {
        BloomFilter *bf = new BloomFilter(num_insert, num_cells);

        for (int i = 0; i < num_insert; i++) {
            bf->Insert(rand() % 1000000);
        }


        auto start = std::chrono::high_resolution_clock::now();
        // Should be all misses + some false positive
        for (int i = 0; i < num_probes; i++) {
            bf->Search(1000000 + rand() % 1000000);
        }
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_cells << "," << num_insert << "," << duration.count() << std::endl;
        //}
    }
}



//void dummy2() {
//    return;
//}
//
//void dummy() {
//    dummy2();
//    return;
//}


void CostOFBloomFilterBuild() {

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_insert_max = 1000000;
    std::cout << "num_insert,duration" << std::endl;
    for (int num_insert = 10000; num_insert<=num_insert_max; num_insert+=10000) {

        start = std::chrono::high_resolution_clock::now();

        BloomFilter *bf = new BloomFilter(num_insert);
        for (int i = 0; i < num_insert; i++) {
            bf->Insert(rand());
        }
        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert << "," << duration.count() << std::endl;
    }


}

// @TODO do multiple runs at each num_insert value
void CostOfHashTableBuild() {

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;



    int num_insert_max = 1000000;

    std::cout << "num_insert,duration" << std::endl;

    for (int num_insert = 10000; num_insert<=num_insert_max; num_insert+=10000) {
        start = std::chrono::high_resolution_clock::now();

        spp::sparse_hash_map<long long, bool> hash;
        for (int i = 0; i < num_insert; i++) {
            hash[rand()] = true;
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert << "," << duration.count() << std::endl;
    }
}

void CostOfBloomFilterProbe(int dummy) {


    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    for (int num_insert=500; num_insert<50000; num_insert+=500) {
        BloomFilter *bf = new BloomFilter(num_insert);


        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            bf->Search(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }


    for (int num_insert=50000; num_insert<1000000; num_insert+=10000) {
        BloomFilter *bf = new BloomFilter(num_insert);


        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            bf->Search(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }

    for (int num_insert=1000000; num_insert<10000000; num_insert+=100000) {
        BloomFilter *bf = new BloomFilter(num_insert);

        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            bf->Search(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }

    for (int num_insert=10000000; num_insert<=30000000; num_insert+=1000000) {
        BloomFilter *bf = new BloomFilter(num_insert);

        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            bf->Search(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }



}




void CostOfHashTableProbe() {
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    for (int num_insert=500; num_insert<50000; num_insert+=500) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);


        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            hash.count(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }


    for (int num_insert=50000; num_insert<1000000; num_insert+=10000) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);


        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            hash.count(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }

    for (int num_insert=1000000; num_insert<10000000; num_insert+=100000) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);


        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            hash.count(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }

    for (int num_insert=10000000; num_insert<=30000000; num_insert+=1000000) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);


        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {
            hash.count(i);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }
}

void ProbeTest(const char* alg, int n) {

    if ((std::string) alg == "bloom") {
        //CostOfBloomFilterProbe(n);
        CostOfHashTableProbe();
    }
    //CostOfHashTableProbe();
    //CostOfBloomFilterProbe();
    //CostOfHashTableBuild();
    //CostOFBloomFilterBuild();
}
