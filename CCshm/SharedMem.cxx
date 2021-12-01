/*	SharedMem.cxx
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#include "SharedMem.h"

bool	CCMapSharedMemory(CCSharedMem_t &shm, bool &use_existing_sizes, bool &force_init, size_t buffer_size, size_t data_offset) {
	use_existing_sizes = false;
	shm.id	= shmget(shm.key, 0, 0);
	if (shm.id >= 0 && !force_init) {
		shmid_ds	info = {};
		shmctl(shm.id, IPC_STAT, &info);
		shm.size = info.shm_segsz;
		use_existing_sizes = true;
	}
	else {
		if (buffer_size == 0) {
			printf("Error, shared memory region doesn't exist yet, and I don't know how large it should be so can't create it m'self\n");
			//	We have no idea what size the buffer should be, so we definitely can't create it.
			return false;
		}
		shm.size	= 4096 + buffer_size;
		printf("Allocating %zu bytes shared memory\n", shm.size);
		shm.id	= shmget(shm.key, shm.size, 0666 | IPC_CREAT);
		if (shm.id < 0) {
			printf("Error creating shared memory region. Deleting existing and making new: %s\n", strerror(errno));
			shmid_ds	ds = {};
			shm.id	= shmget(shm.key, 0, 0);
			shmctl(shm.id, IPC_RMID, &ds);
			shm.id	= shmget(shm.key, shm.size, 0666 | IPC_CREAT);
			if (shm.id < 0) {
				printf("Failed to delete and recreate segment. Exiting: %s\n", strerror(errno));
				return false;
			}
		}
		force_init = true;
	}
	shm.ptr	= (uint8_t*)shmat(shm.id, (void*)0, 0);
	if (shm.ptr) {
		shm.data = shm.ptr + data_offset;
		return true;
	}
	return false;
}


void	CCUnmapSharedMemory(CCSharedMem_t &shm) {
	if (shm.key && shm.ptr) {
		if (shmdt(shm.ptr) != 0)
			printf("Warning, failed to unmap segment at %p\n", shm.ptr);
	}
	shm.ptr = 0;
	shm.data = 0;
}
