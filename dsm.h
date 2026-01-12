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

#define MSG_UPDATE 10
#define MSG_ACK    11

struct WriteMessage {
	char varName[32];
	int value;
};

struct CASMessage {
	char varName[32];
	int expectedValue;
	int newValue;
};

struct CASResponse {
	char varName[32];
	bool result;
};

struct PendingUpdate {
	int timestamp;
	int sender;
	char varname[32];
	int value;
	int expected;
	int newValue;
	int msgType;
	std::set<int> acks;
};

class DSM {
private:
	std::map<std::string, int> data;
	std::mutex mtx;
	std::map<std::string, std::vector<int>> subscriberList;
	std::thread listenerThread;
	std::atomic<bool> keepRunning;
	std::map<std::string, std::promise<CASResponse>> pendingCAS;
	std::mutex casMtx;

	int lamportClock;
	std::mutex orderMtx;
	std::map<std::string, std::vector<PendingUpdate>> holdbackQueue;
	std::map<std::pair<int, int>, std::promise<bool>> casPromises;

public:
	DSM(int argc, char** argv);
	void initTopology();
	void subscribe(std::string varName);

	int read(std::string varName);
	void write(std::string varName, int value);
	bool compareAndExchange(std::string varName, int expectedValue, int newValue);
	void broadcastUpdate(std::string varName, int value);

	void handleWrite(WriteMessage message);
	void handleCAS(CASMessage message, int source);

	int getMyRank();
	int getOwnerOfVariable(std::string varName);

	void close();
	~DSM();

private:
	void listen();
};
