/*	SemRWLock.h
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#ifndef SemRWLock_h
	#define SemRWLock_h

#include <sys/types.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/ipc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/time.h>

#ifdef LINUX
//	This union is defined in system headers on OS X.
union semun {
	int              val;    /* Value for SETVAL */
	struct semid_ds *buf;    /* Buffer for IPC_STAT, IPC_SET */
	unsigned short  *array;  /* Array for GETALL, SETALL */
	struct seminfo  *__buf;  /* Buffer for IPC_INFO
							   (Linux-specific) */
};
#endif

enum {
	kSRW_n_read = 0,
	kSRW_n_write = 1,
	kSRW_can_write = 2
};

typedef int semaphore_id_t;

#if defined(__clang__)
#define atomic_increment(x) __sync_add_and_fetch_4(x, 1)
#define atomic_decrement(x) __sync_sub_and_fetch_4(x, 1)
#elif defined(__GNUC__) || defined(__GNUG__)
static inline int32_t	atomic_increment(volatile int32_t *value) {
	return __sync_add_and_fetch(value, 1);
}

static inline int32_t	atomic_decrement(volatile int32_t *value) {
	return __sync_add_and_fetch(value, -1);
}
#endif

struct CCSemaphore_t {
	key_t 			key;
	semaphore_id_t	sem;
};

class CCObject {
	public:
		void* operator		new(size_t sz) { return calloc(1, sz); };
		void  operator		delete(void *obj) { free(obj); };

		CCObject(void) : ref_count(1) {};
		virtual ~CCObject(void) {};
		
		void*				retain(void) {
			atomic_increment(&ref_count);
			return this;
		};
		void				release(void) {
			int32_t		newCount	= atomic_decrement(&ref_count);
			if (newCount == 0)
				delete this;
		};
		
		int32_t	ref_count;
};

class SemRWLock : public CCObject {
	public:
		SemRWLock(semaphore_id_t sem, int sem_index);
		~SemRWLock(void);
		
		void	init();
		void	readLock();
		void	writeLock();
		void	unlock();
		void	dump();
	protected:
		semaphore_id_t		sem;
		int					sem_index;
		int					state; // 0 == no lock, 1 == read lock, 2 == write lock
		pthread_mutex_t		mutex;
		pthread_cond_t		cond;
		
		void	createOp(sembuf &buf, unsigned short num, short op, short flg);
};


bool	CCSemaphoreGet(CCSemaphore_t &sem, int count, bool &force_init);
double	double_time(void);

#endif

