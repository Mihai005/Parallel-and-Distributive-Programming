#pragma once
#include <mpi.h>
#include <map>
#include <mutex>
#include <thread>
#include <string>
#include <vector>
#include <future>
#include <iostream>
#include <set>
#include <queue>

// Tags
#define TAG_LAMPORT_MSG 10
#define TAG_LAMPORT_ACK 11


struct LamportMessage {
	int timestamp;
	int senderRank;
	char varName[32];
	int value;

	bool isCAS;
	int expectedValue;
	
	bool operator>(const LamportMessage& other) const {
		if (timestamp != other.timestamp)
			return timestamp > other.timestamp;
		return senderRank > other.senderRank;
	}
};

class DSM {
private:
	std::map<std::string, int> data;
	std::map<std::string, bool> subscriptions;
	std::map<std::string, std::vector<int>> subscriberList;
	std::vector<int> myPeers;
	std::mutex mtx;
	std::thread listenerThread;
	std::atomic<bool> keepRunning;

	int lamportClock;
	std::mutex queueMtx;     
	std::vector<int> lastKnownTime;
	std::priority_queue<LamportMessage, std::vector<LamportMessage>, std::greater<LamportMessage>> holdbackQueue;
	std::map<int, std::promise<bool>> casPromises;
	std::mutex casPromiseMtx;

	std::function<void(std::string, int)> updateCallback;

	int rank;
	int size;

public:
	DSM(int argc, char** argv);
	void subscribe(std::string varName);

	int read(std::string varName);
	void write(std::string varName, int value);
	bool compareAndExchange(std::string varName, int expectedValue, int newValue);
	void setCallback(std::function<void(std::string, int)> cb);

	int getMyRank() const { return rank; };
	int getSize() const { return size; }

	void close();
	~DSM();

private:
	void initTopology();
	void listen();
	void tick(int receivedTime = 0);
	void processQueue();
};
