#include "dsm.h"
#include <cstring>
#include <algorithm>
#include <iostream>

DSM::DSM(int argc, char** argv) : keepRunning(true), lamportClock(0)
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided < MPI_THREAD_MULTIPLE) {
        printf("Error: MPI does not support full threading.\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    lastKnownTime.resize(size, 0);

    initTopology();

    listenerThread = std::thread(&DSM::listen, this);
}

DSM::~DSM()
{
    keepRunning = false;
    if (listenerThread.joinable()) {
        listenerThread.join();
    }
    MPI_Finalize();
}

void DSM::initTopology()
{
    subscriberList["var_0"] = { 0, 1 };
    subscriberList["var_1"] = { 1, 2 };
    subscriberList["var_2"] = { 0, 1, 2 };

    for (auto& pair : subscriberList) {
        bool amISub = false;
        for (int s : pair.second) {
            if (s == rank) { amISub = true; break; }
        }
        if (amISub) {
            subscribe(pair.first);
        }
    }
}

void DSM::subscribe(std::string varName)
{
    subscriptions[varName] = true;
    if (data.find(varName) == data.end()) {
        data[varName] = 0;
    }
}

void DSM::setCallback(std::function<void(std::string, int)> cb)
{
    std::lock_guard<std::mutex> lock(mtx);
    updateCallback = cb;
}

int DSM::read(std::string varName)
{
    std::lock_guard<std::mutex> g(mtx);
    if (data.find(varName) != data.end()) return data[varName];
    return 0;
}

void DSM::tick(int receivedTime)
{
    lamportClock = std::max(lamportClock, receivedTime) + 1;
}

void DSM::write(std::string varName, int value)
{
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (subscriptions.find(varName) == subscriptions.end()) {
            printf("[Rank %d] Error: Cannot write to '%s' (Not Subscribed)\n", rank, varName.c_str());
            return;
        }
    }

    int myTime;
    {
        std::lock_guard<std::mutex> lock(queueMtx);
        tick();
        myTime = lamportClock;
        lastKnownTime[rank] = lamportClock;
    }

    LamportMessage msg;
    strcpy(msg.varName, varName.c_str());
    msg.value = value;
    msg.senderRank = rank;
    msg.timestamp = myTime;
    msg.isCAS = false;
    msg.expectedValue = 0;

    if (subscriberList.count(varName)) {
        const std::vector<int>& targets = subscriberList[varName];
        for (int target : targets) {
            MPI_Send(&msg, sizeof(msg), MPI_BYTE, target, TAG_LAMPORT_MSG, MPI_COMM_WORLD);
        }
    }
}

bool DSM::compareAndExchange(std::string varName, int expectedValue, int newValue)
{
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (subscriptions.find(varName) == subscriptions.end()) 
        {
            printf("[Rank %d] Error: Cannot CAS '%s' (Not Subscribed)\n", rank, varName.c_str());
            return false;
        }
    }

    int myTime;
    std::future<bool> resultFuture;

    {
        std::lock_guard<std::mutex> lock(queueMtx);
        tick();
        myTime = lamportClock;
        lastKnownTime[rank] = lamportClock;

        std::lock_guard<std::mutex> pLock(casPromiseMtx);
        casPromises[myTime] = std::promise<bool>();
        resultFuture = casPromises[myTime].get_future();
    }

    LamportMessage msg;
    strcpy(msg.varName, varName.c_str());
    msg.value = newValue;
    msg.senderRank = rank;
    msg.timestamp = myTime;
    msg.isCAS = true;
    msg.expectedValue = expectedValue;

    if (subscriberList.count(varName)) {
        const std::vector<int>& targets = subscriberList[varName];
        for (int target : targets) 
        {
            MPI_Send(&msg, sizeof(msg), MPI_BYTE, target, TAG_LAMPORT_MSG, MPI_COMM_WORLD);
        }
    }

    return resultFuture.get();
}

void DSM::listen()
{
    MPI_Status status;
    int flag = 0;

    while (keepRunning)
    {
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

        if (flag)
        {
            if (status.MPI_TAG == TAG_LAMPORT_MSG)
            {
                LamportMessage msg;
                MPI_Recv(&msg, sizeof(msg), MPI_BYTE, status.MPI_SOURCE, TAG_LAMPORT_MSG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                std::lock_guard<std::mutex> lock(queueMtx);

                tick(msg.timestamp);

                lastKnownTime[status.MPI_SOURCE] = std::max(lastKnownTime[status.MPI_SOURCE], msg.timestamp);

                holdbackQueue.push(msg);

                int ackTime = lamportClock;
                if (subscriberList.count(msg.varName)) {
                    for (int target : subscriberList[msg.varName]) {
                        if (target != rank)
                            MPI_Send(&ackTime, 1, MPI_INT, target, TAG_LAMPORT_ACK, MPI_COMM_WORLD);
                    }
                }

                processQueue();
            }
            else if (status.MPI_TAG == TAG_LAMPORT_ACK)
            {
                int receivedTime;
                MPI_Recv(&receivedTime, 1, MPI_INT, status.MPI_SOURCE, TAG_LAMPORT_ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                std::lock_guard<std::mutex> lock(queueMtx);

                tick(receivedTime);

                lastKnownTime[status.MPI_SOURCE] = std::max(lastKnownTime[status.MPI_SOURCE], receivedTime);

                processQueue();
            }
            else
            {
                MPI_Recv(NULL, 0, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

void DSM::processQueue()
{
    while (!holdbackQueue.empty())
    {
        LamportMessage msg = holdbackQueue.top();

        bool isStable = true;
        bool triggerCallback = false;

        if (subscriberList.count(msg.varName))
        {
            const std::vector<int>& subscribers = subscriberList[msg.varName];
            for (int subRank : subscribers)
            {
                if (subRank == msg.senderRank || subRank == rank) continue;
                if (lastKnownTime[subRank] <= msg.timestamp) {
                    isStable = false;
                    break;
                }
            }
        }
        else 
        {
            isStable = false;
        }

        if (!isStable) break;

        {
            // Execute message
            bool isSubscribed = (subscriptions.count(msg.varName) > 0);

            if (msg.isCAS)
            {
                std::lock_guard<std::mutex> dataLock(mtx);
                int currentVal = isSubscribed ? data[msg.varName] : 0;
                bool success = (isSubscribed && currentVal == msg.expectedValue);
                if (success) data[msg.varName] = msg.value;
                triggerCallback = success;

                if (msg.senderRank == rank && casPromises.count(msg.timestamp)) 
                {
                    std::lock_guard<std::mutex> pLock(casPromiseMtx);
                    casPromises[msg.timestamp].set_value(success);
                    casPromises.erase(msg.timestamp);
                }
            }
            else if (isSubscribed)
            {
                data[msg.varName] = msg.value;
                triggerCallback = true;
            }
        }

        if (triggerCallback && updateCallback) {
            updateCallback(msg.varName, msg.value);
        }

        holdbackQueue.pop();
    }
}