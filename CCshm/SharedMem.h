/*	SharedMem.h
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#ifndef SharedMem_h
	#define SharedMem_h

#include "SemRWLock.h"

struct CCSharedMem_t {
	key_t			key;
	int				id;
	size_t			size;
	uint8_t			*ptr,	//	start of shared memory block
					*data;	//	client can configure data to start after header
};

bool	CCMapSharedMemory(CCSharedMem_t &shm, bool &use_existing_sizes, bool &force_init, size_t buffer_size, size_t data_offset);
void	CCUnmapSharedMemory(CCSharedMem_t &shm);

#endif

