/*	CCshm.h

*/

#ifndef CCshm_h
	#define CCshm_h

#include "SemRWLock.h"

class RWLockTest {
	public:
		RWLockTest(const char *path, bool init);
		virtual ~RWLockTest() {};
		
		void	writer();
		void	reader();
		
		SemRWLock	*lock;
		key_t				semaphore_key,
							memory_key;
		semaphore_id_t	semaphores;
		int					mem_id;
		
		uint32_t			*memory;
		bool				emulateCrash;
};
#endif

