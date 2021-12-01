/*	SemRWLock.cxx
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/
#include "SemRWLock.h"

SemRWLock::SemRWLock(semaphore_id_t sem, int sem_index) : sem(sem), sem_index(sem_index) {
	//	We don't perform any particular initialization. If the semaphores need
	//	to be initialized, that must be done manually, since we don't want every process
	//	to intialize the semaphores. Ideally, that should happen only once, in a process that
	//	never crashes.
	pthread_mutex_init(&mutex, 0);
	pthread_cond_init(&cond, 0);
}


SemRWLock::~SemRWLock() {
	//	The semaphores need no particular cleanup; they are shared resources
	//	after all.
	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&cond);
}


void	SemRWLock::init() {
	/*	The read-write lock relies on three semaphores, starting at sem_index:
		n_read (0): Number of readers (default == 0)
		n_write (1): Number of writers (default == 0)
		can_write (2): Can-write (default == 1)
		
		Taking a read lock means:
			increment n_read
			wait n_write == 0
			wait write == 0
		
		Taking a write lock means:
			increment n_write
			wait n_read == 0
			decrement can_write
		
		Releasing a read lock:
			decrement n_read
		
		Releasing a write lock:
			decrement n_write
			increment can_write
	*/
	semun 	param;
	int 	result;
	param.val = 0;
	result = semctl(sem, sem_index+kSRW_n_read, SETVAL, param);
	result = semctl(sem, sem_index+kSRW_n_write, SETVAL, param);
	param.val = 1;
	result = semctl(sem, sem_index+kSRW_can_write, SETVAL, param);
	
}


void	SemRWLock::dump() {
	for (int i=0;i<3;i++) {
		printf("%d has value %d\n", sem_index+i, semctl(sem, sem_index+i, GETVAL));
	}
}


void	SemRWLock::readLock() {
	pthread_mutex_lock(&mutex);
	while (state != 0)
		pthread_cond_wait(&cond, &mutex);
	
	sembuf op[2];
	createOp(op[0], sem_index+kSRW_n_read, 1, SEM_UNDO);
	createOp(op[1], sem_index+kSRW_n_write, 0, 0);
	//dump();
	//printf("Taking read lock..\n");
	int result = 0;
	do { result = semop(sem, op, sizeof(op)/sizeof(sembuf));
	} while (result != 0 && errno == EINTR);
	//printf("Took read lock\n");
	state = 1;
	pthread_mutex_unlock(&mutex);
}


void	SemRWLock::writeLock() {
	pthread_mutex_lock(&mutex);
	while (state != 0)
		pthread_cond_wait(&cond, &mutex);
	sembuf op[3];
	createOp(op[0], sem_index+kSRW_n_read, 0, 0);
	createOp(op[1], sem_index+kSRW_n_write, 1, SEM_UNDO);
	createOp(op[2], sem_index+kSRW_can_write, -1, SEM_UNDO);
	//printf("Taking write lock..\n");
	int result = 0;
	do { result = semop(sem, op, sizeof(op)/sizeof(sembuf));
	} while (result != 0 && errno == EINTR);
	//printf("Took write lock\n");
	state = 2;
	pthread_mutex_unlock(&mutex);
}


void	SemRWLock::unlock() {
	pthread_mutex_lock(&mutex);
	if (state == 1) {
		//	Release a read lock
		sembuf op[1];
		createOp(op[0], sem_index+kSRW_n_read, -1, SEM_UNDO);
		//printf("Unlock read..\n");
		int result = 0;
		do { result = semop(sem, op, sizeof(op)/sizeof(sembuf));
		} while (result != 0 && errno == EINTR);
		//printf("Did unlock read..\n");
		state = 0;
	}
	else if (state == 2) {
		//	Release a write lock
		sembuf op[2];
		createOp(op[0], sem_index+kSRW_n_write, -1, SEM_UNDO);
		createOp(op[1], sem_index+kSRW_can_write, 1, SEM_UNDO);
		//printf("Unlock write..\n");
		int result = 0;
		do { result = semop(sem, op, sizeof(op)/sizeof(sembuf));
		} while (result != 0 && errno == EINTR);
		//printf("Did unlock write..\n");
		state = 0;
	}
	else
		printf("Error, attempting to unlock, but state is 0\n");
	pthread_cond_broadcast(&cond);
	pthread_mutex_unlock(&mutex);
}


void	SemRWLock::createOp(sembuf &buf, unsigned short num, short op, short flg) {
	buf.sem_num = num;
	buf.sem_op = op;
	buf.sem_flg = flg;
}


bool	CCSemaphoreGet(CCSemaphore_t &sem, int count, bool &force_init) {
	sem.sem = semget(sem.key, count, 0);
	if (sem.sem < 0) {
		force_init = true;
		sem.sem = semget(sem.key, count, 0666|IPC_CREAT);
		if (sem.sem < 0) {
			printf("Error getting semaphore: %s\n", strerror(errno));
			return false;
		}
	}
	return true;
}


double	double_time(void) {
	struct timeval now;
	
	gettimeofday(&now, 0);
	return (double)now.tv_sec + (((double)now.tv_usec)/10e5);
}
