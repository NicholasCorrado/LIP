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

        BloomFilter *bf = new BloomFilter(num_insert, 3);
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





void CostOfHashTableProbe() {
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    for (int num_insert=500; num_insert<50000; num_insert+=500) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);
        for (int i = 0; i < num_insert; i++) {

            hash[rand() % (INT_MAX / 2)] = true;
        }

        long long accu = 0;
        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {

            int random = rand() % INT_MAX + (INT_MAX / 2);

            // Isolate random number generation from probing
            start = std::chrono::high_resolution_clock::now();

            hash.count(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert << "," << accu << std::endl;
    }


    for (int num_insert=50000; num_insert<1000000; num_insert+=10000) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);
        for (int i = 0; i < num_insert; i++) {

            hash[rand() % (INT_MAX / 2)] = true;
        }

        long long accu = 0;
        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {

            int random = rand() % INT_MAX + (INT_MAX / 2);

            // Isolate random number generation from probing
            start = std::chrono::high_resolution_clock::now();

            hash.count(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert << "," << accu << std::endl;
    }

    for (int num_insert=1000000; num_insert<10000000; num_insert+=100000) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);
        for (int i = 0; i < num_insert; i++) {

            hash[rand() % (INT_MAX / 2)] = true;
        }

        long long accu = 0;
        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {

            int random = rand() % INT_MAX + (INT_MAX / 2);

            // Isolate random number generation from probing
            start = std::chrono::high_resolution_clock::now();

            hash.count(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert << "," << accu << std::endl;
    }

    for (int num_insert=10000000; num_insert<=30000000; num_insert+=1000000) {
        spp::sparse_hash_map<long long, bool> hash(num_insert);
        for (int i = 0; i < num_insert; i++) {

            hash[rand() % (INT_MAX / 2)] = true;
        }

        long long accu = 0;
        start = std::chrono::high_resolution_clock::now();

        // Should be all hits
        for (int i = 0; i < num_probes; i++) {

            int random = rand() % INT_MAX + (INT_MAX / 2);

            // Isolate random number generation from probing
            start = std::chrono::high_resolution_clock::now();

            hash.count(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert << "," << accu << std::endl;
    }
}


void HashTableProbeCostMiss() {

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    int num_insert = 500;
    while (num_insert <= 3000000) {

        spp::sparse_hash_map<long long, bool> hash(num_insert);
        for (int i = 0; i < num_insert; i++) {
            hash[rand() % (INT_MAX/2)]=true;
        }

        long long accu = 0;

        // Should be all misses + false positives
        for (int i = 0; i < num_probes; i++) {

            int random = rand() % INT_MAX + (INT_MAX/2);

            // Isolate random number generation from probing
            start = std::chrono::high_resolution_clock::now();

            hash.count(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert<< "," << accu << std::endl;

        if (num_insert < 50000) num_insert += 500;
        else if (num_insert < 1000000) num_insert += 10000;
        else num_insert += 100000; // else if (num_insert < 3000000)
    }
}


void HashTableProbeCostHit() {

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    int num_insert = 500;
    while (num_insert <= 3000000) {

        // Save the random numbers we insert into the filter so we know which values to probe
        // that will always be hits (while still being random)
        int randoms[num_insert];

        spp::sparse_hash_map<long long, bool> hash(num_insert);
        for (int i = 0; i < num_insert; i++) {
            randoms[i] = rand();
            hash[randoms[i]]=true;
        }

        long long accu = 0;

        // Should be all misses + false positives
        for (int i = 0; i < num_probes; i++) {

            // Isolate random number generation from probing
            int random = randoms[i%num_insert];
            start = std::chrono::high_resolution_clock::now();

            hash.count(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert<< "," << accu << std::endl;

        if (num_insert < 50000) num_insert += 500;
        else if (num_insert < 1000000) num_insert += 10000;
        else num_insert += 100000; // else if (num_insert < 3000000)
    }
}


void BloomFilterProbeCostMiss() {

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    int num_insert = 500;
    while (num_insert <= 3000000) {

        BloomFilter *bf = new BloomFilter(num_insert, 3);
        for (int i = 0; i < num_insert; i++) {
            bf->Insert(rand() % (INT_MAX/2));
        }

        long long accu = 0;

        // Should be all misses + false positives
        for (int i = 0; i < num_probes; i++) {

            int random = rand() % INT_MAX + (INT_MAX/2);

            // Isolate random number generation from probing
            start = std::chrono::high_resolution_clock::now();

            bf->Search(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert<< "," << accu << std::endl;

        if (num_insert < 50000) num_insert += 500;
        else if (num_insert < 1000000) num_insert += 10000;
        else num_insert += 100000; // else if (num_insert < 3000000)
    }
}


void BloomFilterProbeCostHit() {

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;
    std::chrono::high_resolution_clock::duration duration;

    int num_probes = 1000000;

    int num_insert = 500;
    while (num_insert <= 3000000) {

        // Save the random numbers we insert into the filter so we know which values to probe
        // that will always be hits (while still being random)
        int randoms[num_insert];

        BloomFilter *bf = new BloomFilter(num_insert, 3);
        for (int i = 0; i < num_insert; i++) {
            randoms[i] = rand();
            bf->Insert(randoms[i]);
        }

        long long accu = 0;

        // Should be all misses + false positives
        for (int i = 0; i < num_probes; i++) {

            // Isolate random number generation from probing
            int random = randoms[i%num_insert];
            start = std::chrono::high_resolution_clock::now();

            bf->Search(random);

            stop = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            accu += duration.count();
        }
        std::cout << num_insert<< "," << accu << std::endl;

        if (num_insert < 50000) num_insert += 500;
        else if (num_insert < 1000000) num_insert += 10000;
        else num_insert += 100000; // else if (num_insert < 3000000)
    }
}






void ProbeTest(const char* alg) {

    if ((std::string) alg == "bloom-hit") {
        BloomFilterProbeCostHit();
    }
    else if ((std::string) alg == "bloom-miss") {
        BloomFilterProbeCostMiss();
    }
    else if ((std::string) alg == "hash-hit") {
        HashTableProbeCostHit();
    }
    else if ((std::string) alg == "hash-miss") {
        HashTableProbeCostMiss();
    }
    else {
        std::cout << "invalid arguments" << std::endl;
    }
    //CostOfHashTableProbe();
    //CostOfBloomFilterProbe();
    //CostOfHashTableBuild();
    //CostOFBloomFilterBuild();
}
