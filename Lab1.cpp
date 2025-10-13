#include <thread>
#include <mutex>
#include <chrono>
#include <vector>
#include <iostream>

constexpr int nrOfWarehouses = 5000;
constexpr int warehouseCapacity = 10;
constexpr int noOfOperations = 100000;
constexpr int noOfThreads = 8;
constexpr int consistencyCheckInterval = 30000;

class Solution
{
	std::vector<std::vector<int>> warehouses;
	std::vector<std::vector<int>> initialWarehouses;
	std::vector<std::mutex> mtxFineGrained;
	std::mutex mtxGlobal;
	int initialStock;
	std::vector<int> initialProductTotals;

public:

	Solution() : warehouses(nrOfWarehouses, std::vector<int>(warehouseCapacity)), mtxFineGrained(nrOfWarehouses), initialProductTotals(warehouseCapacity)
	{
		srand((unsigned int)time(NULL));
		this->initializeStock(); 
		this->initializeInventoryCheck();
	}

	void restoreInitialStock()
	{
		warehouses = initialWarehouses;
	}

private:

	void initializeInventoryCheck()
	{
		for (int j = 0; j < warehouseCapacity; ++j)
		{
			int sum = 0;
			for (int i = 0; i < nrOfWarehouses; ++i)
				sum += warehouses[i][j];

			initialProductTotals[j] = sum;
		}
	}

	void initializeStock()
	{
		int total = 0;
		for (int i = 0; i < nrOfWarehouses; ++i)
			for (int j = 0; j < warehouseCapacity; ++j)
			{
				int quantity = rand() % 500 + 1000;
				warehouses[i][j] = quantity;
				total += quantity;
			}
			
	    initialStock = total;
		initialWarehouses = warehouses;
	}

	int computeTotal() const
	{
		int total = 0;
		for (int i = 0; i < nrOfWarehouses; ++i)
			for (int j = 0; j < warehouseCapacity; ++j)
				total += warehouses[i][j];

		return total;
	}

	bool checkProductTotals()
	{
		for (int product = 0; product < warehouseCapacity; ++product)
		{
			int currentTotal = 0;
			int expectedTotal = 0;

			for (int i = 0; i < nrOfWarehouses; ++i)
			{
				currentTotal += warehouses[i][product];
				expectedTotal += initialWarehouses[i][product];
			}

			if (currentTotal != expectedTotal)
				return false;
		}

		return true;
	}

	void movesBetweenWarehousesFineGrained()
	{
		int src = rand() % nrOfWarehouses;
		int dest = rand() % nrOfWarehouses;
		int product = rand() % warehouseCapacity;
		int amount = rand() % 100 + 1;

		while (dest == src)
			dest = rand() % nrOfWarehouses;

		int first = std::min(src, dest);
		int second = std::max(src, dest);

		std::scoped_lock lock(mtxFineGrained[first], mtxFineGrained[second]);

		if (warehouses[src][product] >= amount)
		{
			warehouses[src][product] -= amount;
			warehouses[dest][product] += amount;
		}
	}

	void movesBetweenWarehousesGlobal()
	{
		int src = rand() % nrOfWarehouses;
		int dest = rand() % nrOfWarehouses;

		while (dest == src)
			dest = rand() % nrOfWarehouses;

		int product = rand() % warehouseCapacity;
		int amount = rand() % 100 + 1;

		std::scoped_lock lock(mtxGlobal);

		if (warehouses[src][product] >= amount)
		{
			warehouses[src][product] -= amount;
			warehouses[dest][product] += amount;
		}
	}


public:

    void testFineGrained()
    {
        std::vector<std::thread> threads;
        threads.reserve(noOfThreads);

        auto start = std::chrono::system_clock::now();

        for (int i = 0; i < noOfThreads; i++)
            threads.emplace_back([this]() {
				for (int op = 0; op < noOfOperations / noOfThreads; ++op)
				{
					this->movesBetweenWarehousesFineGrained();
					if (op % consistencyCheckInterval == 0)
					{
						std::vector<std::unique_lock<std::mutex>> locks;
						locks.reserve(mtxFineGrained.size());
						for (auto& m : mtxFineGrained)
							locks.emplace_back(m);
						if (!this->checkProductTotals())
							std::cout << "Invariant violated!\n";
					}
				}
            });

        for (auto& t : threads)
            t.join();

        auto end = std::chrono::system_clock::now();
        int finalTotal = computeTotal();

        long long duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "Fine-grained mutex:\n";
        std::cout << "Initial total stock: " << this->initialStock << "\n";
        std::cout << "Final total stock:   " << finalTotal << "\n";
        std::cout << "Time elapsed : " << duration << "ms\n";
        std::cout << (this->initialStock == finalTotal ? "Invariant preserved!" : "Invariant violated!") << "\n\n";
    }

	void testGlobal()
	{
		std::vector<std::thread> threads;
		threads.reserve(noOfThreads);

		auto start = std::chrono::system_clock::now();

		for (int i = 0; i < noOfThreads; i++)
			threads.emplace_back([this]() {
				for (int op = 0; op < noOfOperations / noOfThreads; ++op)
				{
					this->movesBetweenWarehousesGlobal();
					if (op % consistencyCheckInterval == 0)
					{
						std::unique_lock lock(mtxGlobal);
						if (!this->checkProductTotals())
							std::cout << "Invariant violated!\n";
					}
				}
			});

		for (auto& t : threads)
			t.join();

		auto end = std::chrono::system_clock::now();
		int finalTotal = computeTotal();

		long long duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

		std::cout << "Global mutex:\n";
		std::cout << "Initial total stock: " << this->initialStock << "\n";
		std::cout << "Final total stock:   " << finalTotal << "\n";
		std::cout << "Time elapsed: " << duration << "ms\n";
		std::cout << (this->initialStock == finalTotal ? "Invariant preserved!" : "Invariant violated!") << "\n\n";
	}
};

int main()
{
	Solution s;

	s.testFineGrained();
	s.restoreInitialStock();
	s.testGlobal();

	return 0;
}


// bonus problem with try-lock mutex
// livelock
// conditional variable
// optional<T> dequeue(); return optional if dequeue is empty and no more writers