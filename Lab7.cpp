#include <mpi.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <vector>

using namespace std;

using ll = long long;

void setupA(vector<ll> & A, int size)
{
    A.resize(size);
    for (int i = 0; i < size; ++i) {
        A[i] = (i % 5) + 1;
    }
}

void setupB(vector<ll>& B, int size)
{
    B.resize(size);
    for (int i = 0; i < size; ++i) {
        B[i] = (i % 5) + 1;
    }
}

vector<ll> run_regular(int world_rank, int world_size, vector<ll>& A, vector<ll>& B)
{
    vector<ll> C;
    int B_size;

    if (world_rank == 0) B_size = B.size();

    MPI_Bcast(&B_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (world_rank != 0) B.resize(B_size);

    MPI_Bcast(B.data(), B.size(), MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    if (world_rank == 0)
    {
        int A_size = A.size();
        MPI_Bcast(&A_size, 1, MPI_INT, 0, MPI_COMM_WORLD);

        int chunkSize = A.size() / world_size;

        vector<int> send_counts(world_size);
        vector<int> send_displs(world_size);

        int send_offset = 0;
        for (int i = 0; i < world_size; ++i)
        {
            send_displs[i] = send_offset;
            send_counts[i] = (i == world_size - 1) ? A.size() - send_offset : chunkSize;
            send_offset += send_counts[i];
        }

        MPI_Scatterv(A.data(), send_counts.data(), send_displs.data(), MPI_LONG_LONG,
            MPI_IN_PLACE, 0, MPI_DATATYPE_NULL, 0, MPI_COMM_WORLD);

        C.resize(send_counts[0] + B.size() - 1, 0);
        for (int i = 0; i < send_counts[0]; ++i)
            for (int j = 0; j < B.size(); ++j)
                C[i + j] += A[i] * B[j];

        vector<int> recv_counts(world_size);
        vector<int> recv_displs(world_size);
        int total_gather_size = 0;

        for (int i = 0; i < world_size; ++i)
        {
            recv_displs[i] = total_gather_size;
            recv_counts[i] = send_counts[i] + B.size() - 1;
            total_gather_size += recv_counts[i];
        }

        vector<ll> staging_buffer(total_gather_size);
        std::copy(C.begin(), C.end(), staging_buffer.begin());

        MPI_Gatherv(MPI_IN_PLACE, 0, MPI_DATATYPE_NULL,
            staging_buffer.data(), recv_counts.data(), recv_displs.data(), MPI_LONG_LONG, 0, MPI_COMM_WORLD);

        vector<ll> globalC(A.size() + B.size() - 1, 0);
        for (int i = 0; i < world_size; ++i)
        {
            int math_start_index = send_displs[i];
            int staging_start_index = recv_displs[i];

            for (int k = 0; k < recv_counts[i]; ++k)
            {
                globalC[math_start_index + k] += staging_buffer[staging_start_index + k];
            }
        }

        return globalC;
    }
    else
    {
        int A_size;
        MPI_Bcast(&A_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        int expectedChunk = A_size / world_size;
        if (world_rank == world_size - 1)
            expectedChunk = A_size - world_rank * expectedChunk;

        A.resize(expectedChunk);

        MPI_Scatterv(NULL, NULL, NULL, MPI_LONG_LONG,
            A.data(), expectedChunk, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

        C.resize(A.size() + B.size() - 1, 0);
        for (int i = 0; i < A.size(); ++i)
            for (int j = 0; j < B.size(); ++j)
                C[i + j] += A[i] * B[j];

        MPI_Gatherv(C.data(), C.size(), MPI_LONG_LONG,
            NULL, NULL, NULL, MPI_DATATYPE_NULL, 0, MPI_COMM_WORLD);

        return {};
    }
}

vector<ll> sequential_multiply(const vector<ll>& A, const vector<ll>& B) {
    if (A.empty() || B.empty()) return {};
    vector<ll> res(A.size() + B.size() - 1, 0);
    for (size_t i = 0; i < A.size(); ++i)
        for (size_t j = 0; j < B.size(); ++j)
            res[i + j] += A[i] * B[j];
    return res;
}

vector<ll> add_vectors(const vector<ll>& A, const vector<ll>& B) {
    size_t n = max(A.size(), B.size());
    vector<ll> res(n, 0);
    for (size_t i = 0; i < n; ++i) {
        if (i < A.size()) res[i] += A[i];
        if (i < B.size()) res[i] += B[i];
    }
    return res;
}

vector<ll> subtract_vectors(const vector<ll>& A, const vector<ll>& B) {
    size_t n = max(A.size(), B.size());
    vector<ll> res(n, 0);
    for (size_t i = 0; i < n; ++i) {
        if (i < A.size()) res[i] += A[i];
        if (i < B.size()) res[i] -= B[i];
    }
    return res;
}

vector<ll> run_karatsuba(MPI_Comm comm, vector<ll> A, vector<ll> B)
{
    int rank, size;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    long n = A.size();
    MPI_Bcast(&n, 1, MPI_LONG, 0, comm);

    if (size < 3 || n < 1024) {
        if (rank == 0) return sequential_multiply(A, B);
        else return {};
    }

    int leader_g1 = size / 3;
    int leader_g2 = (size / 3) * 2;

    int m = n / 2;

    if (rank == 0) {
        vector<ll> A0(A.begin(), A.begin() + m);
        vector<ll> A1(A.begin() + m, A.end());
        vector<ll> B0(B.begin(), B.begin() + m);
        vector<ll> B1(B.begin() + m, B.end());

        vector<ll> SA = add_vectors(A0, A1);
        vector<ll> SB = add_vectors(B0, B1);

        long szA1 = A1.size(), szB1 = B1.size();
        MPI_Send(&szA1, 1, MPI_LONG, leader_g1, 10, comm);
        MPI_Send(A1.data(), szA1, MPI_LONG_LONG, leader_g1, 11, comm);
        MPI_Send(&szB1, 1, MPI_LONG, leader_g1, 12, comm);
        MPI_Send(B1.data(), szB1, MPI_LONG_LONG, leader_g1, 13, comm);

        long szSA = SA.size(), szSB = SB.size();
        MPI_Send(&szSA, 1, MPI_LONG, leader_g2, 20, comm);
        MPI_Send(SA.data(), szSA, MPI_LONG_LONG, leader_g2, 21, comm);
        MPI_Send(&szSB, 1, MPI_LONG, leader_g2, 22, comm);
        MPI_Send(SB.data(), szSB, MPI_LONG_LONG, leader_g2, 23, comm);

        A = A0;
        B = B0;
    }
    else if (rank == leader_g1) {
        long szA, szB;
        MPI_Recv(&szA, 1, MPI_LONG, 0, 10, comm, MPI_STATUS_IGNORE);
        A.resize(szA);
        MPI_Recv(A.data(), szA, MPI_LONG_LONG, 0, 11, comm, MPI_STATUS_IGNORE);

        MPI_Recv(&szB, 1, MPI_LONG, 0, 12, comm, MPI_STATUS_IGNORE);
        B.resize(szB);
        MPI_Recv(B.data(), szB, MPI_LONG_LONG, 0, 13, comm, MPI_STATUS_IGNORE);
    }
    else if (rank == leader_g2) {
        long szA, szB;
        MPI_Recv(&szA, 1, MPI_LONG, 0, 20, comm, MPI_STATUS_IGNORE);
        A.resize(szA);
        MPI_Recv(A.data(), szA, MPI_LONG_LONG, 0, 21, comm, MPI_STATUS_IGNORE);

        MPI_Recv(&szB, 1, MPI_LONG, 0, 22, comm, MPI_STATUS_IGNORE);
        B.resize(szB);
        MPI_Recv(B.data(), szB, MPI_LONG_LONG, 0, 23, comm, MPI_STATUS_IGNORE);
    }
    else {
        A.clear();
        B.clear();
    }

    int group;
    if (rank < size / 3) group = 0;
    else if (rank < 2 * size / 3) group = 1;
    else group = 2;

    MPI_Comm sub_comm;
    MPI_Comm_split(comm, group, rank, &sub_comm);

    vector<ll> local_result = run_karatsuba(sub_comm, A, B);

    MPI_Comm_free(&sub_comm);

    vector<ll> final_result;

    if (rank == 0) {
        vector<ll> P0 = local_result;
        vector<ll> P1, P2;
        long sz;

        MPI_Recv(&sz, 1, MPI_LONG, leader_g1, 100, comm, MPI_STATUS_IGNORE);
        P1.resize(sz);
        MPI_Recv(P1.data(), sz, MPI_LONG_LONG, leader_g1, 101, comm, MPI_STATUS_IGNORE);

        MPI_Recv(&sz, 1, MPI_LONG, leader_g2, 200, comm, MPI_STATUS_IGNORE);
        P2.resize(sz);
        MPI_Recv(P2.data(), sz, MPI_LONG_LONG, leader_g2, 201, comm, MPI_STATUS_IGNORE);

        vector<ll> mid = subtract_vectors(P2, P0);
        mid = subtract_vectors(mid, P1);

        final_result.resize(n * 2, 0);

        for (size_t i = 0; i < P0.size(); ++i) final_result[i] += P0[i];
        for (size_t i = 0; i < mid.size(); ++i) final_result[i + m] += mid[i];
        for (size_t i = 0; i < P1.size(); ++i) final_result[i + 2 * m] += P1[i];

        while (final_result.size() > 1 && final_result.back() == 0) final_result.pop_back();
    }
    else if (rank == leader_g1 || rank == leader_g2) {
        long sz = local_result.size();
        MPI_Send(&sz, 1, MPI_LONG, 0, (rank == leader_g1 ? 100 : 200), comm);
        MPI_Send(local_result.data(), sz, MPI_LONG_LONG, 0, (rank == leader_g1 ? 101 : 201), comm);
    }

    return final_result;
}

bool check_corectness(vector<ll>& R1, vector<ll>& R2)
{
    if (R1.size() != R2.size())
        return false;

    for (int i = 0; i < R1.size(); ++i)
        if (R1[i] != R2[i])
            return false;

    return true;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    vector<ll> A_global, B_global;

    if (world_rank == 0) 
    {
        int size = 100001;
        setupB(B_global, size);
        setupA(A_global, size);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double start_time = MPI_Wtime();

    vector<ll> R1 = run_regular(world_rank, world_size, A_global, B_global);

    MPI_Barrier(MPI_COMM_WORLD);
    double end_time = MPI_Wtime();

    if (world_rank == 0) 
    {
        cout << "Total Execution Time: " << (end_time - start_time) << " seconds." << endl;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();

    vector<ll> R2 = run_karatsuba(MPI_COMM_WORLD, A_global, B_global);

    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();

    if (world_rank == 0)
    {
        cout << "Total Execution Time: " << (end_time - start_time) << " seconds." << endl;
    }

    if (world_rank == 0)
    {
        if (check_corectness(R1, R2))
        {
            cout << "Results are correct" << endl;
        }
        else
        {
            cout << "Results are not correct" << endl;
        }
    }

    MPI_Finalize();
    return 0;
}