/*	CCshm.cxx
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
	
	Idea for synchronizing producers and consumers:
	Whenever a producer posts a message, the semaphore's value is set to 0, immediately followed by +1
	 => this unblocks any waiting processes, allowing them to process the new message
	This should also work for multiple producers.
	
	Now, what about the message buffer? We need some way of synchronizing access there.
*/
#include "CCshm.h"
#include "SemRWLock.h"
#include "EventBus.h"
#include <pthread.h>

static void* receive_thread(void *arg) {
	EventBus	*bus = (EventBus*)arg;
	printf("Receiver running\n");
	while (true) {
		EventBusData	data = {};
		if (bus->get(data)) {
			printf("%3d: %s\n", bus->wakeup_count, (char*)data.data);
			free(data.data);
		}
		//else
		//	printf("No data\n");
	}
	return 0;
}

static void* receive_thread_many(void *arg) {
	EventBus	*bus = (EventBus*)arg;
	printf("Receiver running [many]\n");
	while (true) {
		std::vector<EventBusData>	items;
		bus->get_many(items);
		for (size_t i=0;i<items.size();i++) {
			printf("%3d: %s\n", bus->wakeup_count, (char*)items[i].data);
			free(items[i].data);
		}
	}
	return 0;
}


int main(int argc, const char * argv[]) {
	bool	init = false,
			auto_post = false,
			dump = false,
			many = false,
            init_locks = false;
	size_t	item_size = 0;
	const char	*path = "./.ccshm-shared-mem-id";
	for (ssize_t i=1;i<argc;i++) {
		if (strcmp(argv[i], "--init") == 0) 
			init = true;
		else if (strcmp(argv[i], "--path") == 0 && i+1 < argc)
			path = argv[++i];
		else if (strcmp(argv[i], "--post") == 0)
			auto_post = true;
		else if (strcmp(argv[i], "--dump") == 0)
			dump = true;
		else if (strcmp(argv[i], "--many") == 0)
			many = true;
		else if (strcmp(argv[i], "--size") == 0)
			item_size = atoi(++argv[i]);
        else if (strcmp(argv[i], "--init-locks") == 0) {
            init_locks = true;
            dump = true;
        }
	}
	struct stat st_buf = {};
	if (stat(path, &st_buf) != 0) {
		printf("Please provide a path to an (empty) file to use for generating shared memory and semaphores.\n");
		exit(-1);
	}
	
	EventBus	*bus = new EventBus(path, 
									0,
									item_size);
	bus->open(init);
	if (dump) {
        printf("Current semaphore state:\n");
		bus->dump();
        if (init_locks) {
            bus->init_locks();
            printf("Semaphores after initialization:\n");
            bus->dump();
        }
		exit(0);
	}
	pthread_t tid;
	pthread_create(&tid, 0, many ? receive_thread_many : receive_thread, bus);
	pthread_detach(tid);
	if (auto_post) {
		char	text[2048];
		for (size_t i=0;i<100;i++) {
			snprintf(text, 2048, "%d: %zu %d\n", getpid(), i, rand() % 1000);
			EventBusData	data = { strlen(text) + 1, text, false };
			bus->post(data);
		}
		printf("Posting-many");
		std::vector<EventBusData>	items;
		for (size_t i=0;i<100;i++) {
			snprintf(text, 2048, "%d: %zu %d | many\n", getpid(), i, rand() % 1000);
			EventBusData	data = { strlen(text) + 1, strdup(text), true };
			items.push_back(data);
		}
		bus->post_many(items);
		
	}
	else {
		while (true) {
			char	buf[1024],
					text[2048];
			fgets(buf, 1023, stdin);
			snprintf(text, 2048, "%d: %s", getpid(), buf);
			EventBusData	data = { strlen(text) + 1, text };
			bus->post(data);
		}
	}
	#if 0
	
	RWLockTest	*tester = new RWLockTest("/Users/daniels/Norut/ngv/CCshm/.ccshm-shared-mem-id", init);
	if (writer)
		tester->writer();
	else
		tester->reader();
	
	#endif
	#if 0
	EventBus	*bus = new EventBus("/Users/daniels/Norut/ngv/CCshm/.ccshm-shared-mem-id");
	int			result = 0;
	
	for (size_t i=1;i<argc;i++) {
		if (strcmp(argv[i], "--reset") == 0) { 
			bus->reset();
		}
		else if (strcmp(argv[i], "--up") == 0) {
			bus->up();
		}
		else if (strcmp(argv[i], "--down") == 0) {
			bus->down();
		}
		else if (strcmp(argv[i], "--zero") == 0) {
			bus->wait_for_zero();
		}
		else if (strcmp(argv[i], "--destroy") == 0) {
			bus->destroy();
		}
	}
	result = bus->get();
	printf("Value of semaphore: %d\n", result);
	#endif
	return 0;
}

#if 0
EventBus::EventBus(const char *path) {
	semaphore_key = ftok(path, 1);
	memory_key = ftok(path, 2);
	printf("Semaphore key: %d Memory key: %d\n", semaphore_key, memory_key);
	semaphores = semget(semaphore_key, 256, 0666|IPC_CREAT);
	printf("Semaphore ID: %d\n", semaphores);
	if (semaphores < 0)
		printf("Error: %s\n", strerror(errno));
}


void	EventBus::up(void) {
	sembuf op;
	
	op.sem_num = 0;
	op.sem_op = 1;
	op.sem_flg = 0;
	int result = semop(semaphores, &op, 1);
	printf("Result: %d %s\n", result, strerror(errno));
}


void	EventBus::down(void) {
	sembuf op;
	
	op.sem_num = 0;
	op.sem_op = -1;
	op.sem_flg = 0;
	int result = semop(semaphores, &op, 1);
	printf("Result: %d %s\n", result, strerror(errno));
}


void	EventBus::wait_for_zero(void) {
	sembuf op;
	op.sem_num = 0;
	op.sem_op = 0;
	op.sem_flg = 0;
	int result = semop(semaphores, &op, 1);
	printf("Result: %d %s\n", result, strerror(errno));
}


void	EventBus::reset(void) {
	semun 	op;
	op.val = 0;
	int result = semctl(semaphores, 0, SETVAL, op);
	printf("Result: %d %s\n", result, strerror(errno));
}



int		EventBus::get(void) {
	return semctl(semaphores, 0, GETVAL);	
}


void	EventBus::destroy(void) {
	int result = semctl(semaphores, 0, IPC_RMID);
	printf("destroyed: %d %s\n", result, strerror(errno));
}
#endif


RWLockTest::RWLockTest(const char *path, bool init) {
	semaphore_key = ftok(path, 1);
	memory_key = ftok(path, 2);
	printf("Semaphore key: %d Memory key: %d\n", semaphore_key, memory_key);
	semaphores = semget(semaphore_key, 256, 0666|IPC_CREAT);
	lock = new SemRWLock(semaphores, 1);
	
	mem_id	= shmget(memory_key, 4096, 0666 | IPC_CREAT);
	memory	= (uint32_t*)shmat(mem_id, 0, 0);
	if (init) {
		printf("Initializing\n");
		lock->init();
		memory[0] = 0;
		emulateCrash = true;
	}
	srand(0);
}


void	RWLockTest::reader() {
	int rounds = 100;
	printf("Reading..\n");
	while ((rounds--)>0) {
		int sleep_time = 1000; //((rand() & 0x7ffffff) % 100000);
		usleep(sleep_time);
		printf("Locking..\n");
		lock->readLock();
		printf("Reader: %d\n", memory[0]);
		if (rounds == 1) {
			printf("Crashing with lock\n");
			exit(-1);
		}
		lock->unlock();
	}
}


void	RWLockTest::writer() {
	int rounds = 1000;
	while ((rounds--) > 0) {
		int sleep_time = 1000; //((rand() & 0x7ffffff) % 100000);
		usleep(sleep_time);
		lock->writeLock();
		printf("Writer: %d -> %d\n", memory[0], memory[0]+1);
		memory[0]	+= 1;
		if (emulateCrash && rounds == 1) {
			printf("Crash succedded\n");
			exit(-1);
		}
		lock->unlock();
	}
	printf("Writer exiting. Reading output\n");
	lock->readLock();
	printf("Writer after finish: %dn", memory[0]);
	lock->unlock();
}

