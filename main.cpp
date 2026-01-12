#include <iostream>
#include <mpi.h>
#include <string>
#include <thread>
#include <chrono>
#include "dsm.h"

void log(int rank, std::string msg) {
    printf("[Rank %d] %s\n", rank, msg.c_str());
}

void sleep_ms(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

int main(int argc, char** argv)
{
    DSM dsm(argc, argv);
    int rank = dsm.getMyRank();

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) log(rank, "--- STARTING DSM TEST SUITE ---");

    // -------------------------------
    // Test 1: Partial subscription
    // -------------------------------
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) log(rank, "--- TEST 1: PARTIAL SUBSCRIPTION ---");
    if (rank == 0) dsm.write("var_0", 111);
    sleep_ms(200);

    if (rank == 1) log(rank, "Read var_0: " + std::to_string(dsm.read("var_0")) + " (Expected: 111)");
    if (rank == 2) log(rank, "Read var_0: " + std::to_string(dsm.read("var_0")) + " (Expected: 0)");

    // -------------------------------
    // Test 2: Permission check
    // -------------------------------
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) log(rank, "--- TEST 2: PERMISSION CHECK ---");
    if (rank == 0) dsm.write("var_1", 888);
    sleep_ms(200);

    if (rank == 0) log(rank, "Read var_1: " + std::to_string(dsm.read("var_1")) + " (Expected: 0)");
    if (rank == 2) log(rank, "Read var_1: " + std::to_string(dsm.read("var_1")) + " (Expected: 0)");

    // -------------------------------
    // Test 3: Global variable
    // -------------------------------
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) log(rank, "--- TEST 3: GLOBAL VARIABLE ---");
    if (rank == 2) dsm.write("var_2", 999);
    sleep_ms(200);

    log(rank, "Final var_2: " + std::to_string(dsm.read("var_2")) + " (Expected: 999)");

    // -------------------------------
    // Test 4: Topology ping-pong
    // -------------------------------
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) log(rank, "--- TEST 4: PING-PONG CHAIN ---");

    if (rank == 0) { sleep_ms(100); dsm.write("var_0", 100); }
    else if (rank == 1) {
        int val = 0;
        while (val != 100) { val = dsm.read("var_0"); sleep_ms(10); }
        log(rank, "Forwarding 200 to var_1...");
        dsm.write("var_1", 200);
    }
    else if (rank == 2) {
        int val = 0;
        while (val != 200) { val = dsm.read("var_1"); sleep_ms(10); }
        log(rank, "Chain complete.");
    }

    return 0;
}