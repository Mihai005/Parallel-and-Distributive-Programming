#include "dsm.h"

DSM::DSM(int argc, char**argv)
{
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

	if (provided < MPI_THREAD_MULTIPLE) 
	{
		printf("Error: The MPI library does not support full threading.\n");
		MPI_Abort(MPI_COMM_WORLD, 1);
	}
	
	lamportClock = 0;
	initTopology();
	keepRunning = true;
	listenerThread = std::thread(&DSM::listen, this);
}

void DSM::initTopology()
{
	subscriberList["var_0"] = { 0, 1 };
	subscriberList["var_1"] = { 1, 2 };
	subscriberList["var_2"] = { 0, 1, 2 };
}

void DSM::subscribe(std::string varName)
{
	if (data.find(varName) == data.end())
	{
		data[varName] = 0;
	}
}

DSM::~DSM()
{
	keepRunning = false;

	if (listenerThread.joinable()) 
	{
		listenerThread.join();
	}

	MPI_Finalize();
}

int DSM::getMyRank()
{
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	return rank;
}

int DSM::getOwnerOfVariable(std::string varName)
{
	return varName[4] - '0';
}

int DSM::read(std::string varName)
{
	std::lock_guard<std::mutex> g(mtx);
	return data[varName];
}

void DSM::write(std::string varName, int value)
{   
	{
		std::lock_guard<std::mutex> lock(mtx);
		if (data.find(varName) == data.end())
		{
			return;
		}
	}

	WriteMessage m;
	strcpy(m.varName, varName.c_str());
	m.value = value;
	int myRank = getMyRank();
	int destRank = getOwnerOfVariable(varName);

	if (myRank == destRank)
	{
		{
			std::lock_guard<std::mutex> lock(mtx);
			data[varName] = value;
		}
		
		std::vector<int>& subscribers = subscriberList[varName];
		for (const int& s : subscribers)
		{
			if (s == myRank) continue;
			MPI_Send(&m, sizeof(m), MPI_BYTE, s, 0, MPI_COMM_WORLD); // 0 means I'm the leader and I broadcast the change
		}
	}
	else 
	{
		MPI_Send(&m, sizeof(m), MPI_BYTE, destRank, 1, MPI_COMM_WORLD); // 1 means I'm the worker and I try to update the leaders variable
	}
}

void DSM::broadcastUpdate(std::string varName, int value)
{
	WriteMessage m;
	strcpy(m.varName, varName.c_str());
	m.value = value;

	std::vector<int>& subscribers = subscriberList[varName];
	for (const int& s : subscribers)
	{
		if (s == getMyRank()) continue;
		MPI_Send(&m, sizeof(m), MPI_BYTE, s, 0, MPI_COMM_WORLD); // 0 means I'm the leader and I broadcast the change
	}
}

bool DSM::compareAndExchange(std::string varName, int expectedValue, int newValue)
{
	int myRank = getMyRank();
	int destRank = getOwnerOfVariable(varName);

	{
		std::lock_guard<std::mutex> lock(mtx);
		if (data.find(varName) == data.end())
		{
			return false;
		}
	}

	if (myRank == destRank)
	{
		std::lock_guard<std::mutex> lock(mtx);
		
		if (data[varName] == expectedValue)
		{
			data[varName] = newValue;
			broadcastUpdate(varName, newValue);
			return true;
		}

		return false;
	}

	std::future<CASResponse> futureResult;
	{
		std::lock_guard<std::mutex> casLock (casMtx);
		pendingCAS[varName] = std::promise<CASResponse>();
		futureResult = pendingCAS[varName].get_future();
	}

	CASMessage message;
	strcpy(message.varName, varName.c_str());
	message.expectedValue = expectedValue;
	message.newValue = newValue;

	MPI_Send(&message, sizeof(message), MPI_BYTE, destRank, 2, MPI_COMM_WORLD); // tag 2 for sending CAS request to owner of variable

	return futureResult.get().result;
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
			switch (status.MPI_TAG)
			{
			case 0:
			{
				WriteMessage msg;
				MPI_Recv(&msg, sizeof(msg), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				handleWrite(msg);
				break;
			}

			case 1:
			{
				WriteMessage msg;
				MPI_Recv(&msg, sizeof(msg), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				write(msg.varName, msg.value);
				break;
			}

			case 2:
			{
				CASMessage msg;
				MPI_Recv(&msg, sizeof(msg), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				handleCAS(msg, status.MPI_SOURCE);
				break;
			}

			case 3:
			{
				CASResponse response;
				MPI_Recv(&response, sizeof(response), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				std::lock_guard<std::mutex> casLock(casMtx);

				if (pendingCAS.count(response.varName))
				{
					pendingCAS[response.varName].set_value(response);
					pendingCAS.erase(response.varName);
				}

				break;
			}

			default:
				std::cout << "Unknown tag received: " << status.MPI_TAG << "\n";
				MPI_Recv(NULL, 0, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				break;
			}
		}
		else
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
}

void DSM::handleWrite(WriteMessage message)
{
	std::lock_guard<std::mutex> lock(mtx);
	data[message.varName] = message.value;
}

void DSM::handleCAS(CASMessage message, int source)
{
	CASResponse response;
	strcpy(response.varName, message.varName);
	response.result = false;
	{
		std::lock_guard<std::mutex> lock(mtx);
		if (data[message.varName] == message.expectedValue)
		{
			data[message.varName] = message.newValue;
			broadcastUpdate(message.varName, message.newValue);
			response.result = true;
		}
	}

	MPI_Send(&response, sizeof(response), MPI_BYTE, source, 3, MPI_COMM_WORLD); // tag 3 for sending CAS result
}