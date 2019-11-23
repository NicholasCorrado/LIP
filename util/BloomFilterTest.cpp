//
// Created by Nicholas Corrado on 11/23/19.
//

#include "BloomFilterTest.h"
#include <iostream>
#include "./../BloomFilter.h"
#include "sparsepp/spp.h"

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

void CostOfBloomFilterProbe(int num_insercggdgdg) {


    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 100000;

    for (int num_insert=100; num_insert<10000000; num_insert*=1.1) {
        BloomFilter *bf = new BloomFilter(num_insert);
        for (int i = 0; i < num_insert; i++) {
            bf->Insert(rand() % 10000000);
        }

        start = std::chrono::high_resolution_clock::now();

        // Should be all misses + some false positive
        for (int i = 0; i < num_probes; i++) {
            bf->Search(10000000 + rand() % 10000000);
        }

        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        std::cout << num_insert<< "," << duration.count() << std::endl;

    }

}




void CostOfHashTableProbe() {
    /*
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 100000;
    int num_cells_max = 1000000; // 1mil

    int num_trials = 1;
    int num_cells = 100;
    //std::cout << "num_insert,avg_duration" << std::endl;
    while(num_cells <= num_cells_max) {
        int duration_total = 0;
        for (int trial=0; trial<num_trials; trial++) {

            spp::sparse_hash_map<long long, bool> hash;
            for (int i = 0; i < num_insert; i++) {
                hash[rand()] = true;
            }
            for (int i = 0; i < num_insert; i++) {
                bf->Insert(rand() % 10000000);
            }

            start = std::chrono::high_resolution_clock::now();

            // Should be all misses + some false positive
            for (int i = 0; i < num_probes; i++) {
                bf->Search(10000000 + rand() % 10000000);
            }

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
            duration_total += duration.count();
        }

        std::cout << num_cells << "," << duration.count() / num_trials << std::endl;

        if (num_cells < num_insert) {
            num_cells += num_insert / 50;
        } else {
            num_cells *= 1.1;
        }

    }
     */
}

void ProbeTest(const char* alg, int n) {

    if ((std::string) alg == "bloom") {
        CostOfBloomFilterProbe(n);
    }
    //CostOfHashTableProbe();
    //CostOfBloomFilterProbe();
    //CostOfHashTableBuild();
    //CostOFBloomFilterBuild();
}
