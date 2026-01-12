#include <iostream>
#include <mpi.h>
#include "dsm.h"
#include <string>

int main(int argc, char** argv)
{
    DSM dsm(argc, argv);

    int rank = dsm.getMyRank();

    if (rank == 0) { dsm.subscribe("var_0"); dsm.subscribe("var_2"); }
    if (rank == 1) { dsm.subscribe("var_0"); dsm.subscribe("var_1"); dsm.subscribe("var_2"); }
    if (rank == 2) { dsm.subscribe("var_1"); dsm.subscribe("var_2"); }

    MPI_Barrier(MPI_COMM_WORLD);
   
    if (rank == 0)
    {
        printf("\n[Rank 0] Writing 111 to var_0\n");
        dsm.write("var_0", 111);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (rank == 1)
    {
        printf("[Rank 1] Read var_0: %d (Expected: 111)\n", dsm.read("var_0"));
    }
    if (rank == 2)
    {
        // Rank 2 isn't subscribed. It never gets the message.
        // Reading it returns 0 (default map value).
        printf("[Rank 2] Read var_0: %d (Expected: 0 / Not 111)\n", dsm.read("var_0"));
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // ---------------------------------------------------------
    // TEST 2: BLIND WRITE
    // Rank 0 writes 888 to var_1 (Owner is 1).
    // Rank 0 is NOT subscribed. It sends request, but gets no update back.
    // Rank 2 IS subscribed. It should get the update.
    // ---------------------------------------------------------

    if (rank == 0)
    {
        printf("\n[Rank 0] Writing 888 to var_1 (I am not a subscriber)\n");
        dsm.write("var_1", 888);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (rank == 2)
    {
        printf("[Rank 2] Read var_1: %d (Expected: 0 / Not 888)\n", dsm.read("var_1"));
    }
    if (rank == 0)
    {
        // Rank 0 sent the write request, but is not in the subscriber list.
        // It never received the 'Tag 0' update msg. 
        // Local value remains default (0).
        printf("[Rank 0] Read var_1: %d (Expected: 0 / Not 888)\n", dsm.read("var_1"));
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // ---------------------------------------------------------
    // TEST 3: GLOBAL VARIABLE
    // Everyone subscribes to var_2.
    // ---------------------------------------------------------

    if (rank == 2)
    {
        printf("\n[Rank 2] Writing 999 to var_2 (Global)\n");
        dsm.write("var_2", 999);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    printf("[Rank %d] Final var_2: %d (Expected: 999)\n", rank, dsm.read("var_2"));

    return 0;
}
