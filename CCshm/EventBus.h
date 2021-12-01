/*	EventBus.h
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#ifndef EventBus_h
	#define EventBus_h

#include "SemRWLock.h"
#include "SharedMem.h"
#include <vector>

#define kEventBusMagic	0xd5ffabcdef0102d5

struct EventBusMessage_t {
	uint64_t	magic,
				serial;
	uint32_t	flags;
	pid_t		pid;
	double		timestamp;
	size_t		length;
	uint8_t		data[0];
};

struct EventBusMemoryHeader_t {
	volatile size_t		head;
	volatile uint64_t	serial;
	volatile size_t		num_items,	//	These are the parameters used when the segment was created.
						item_size;
	uint8_t				extra[0];
};


struct EventBusData {
	size_t	length;
	void	*data;
	bool	free_after;
};


class EventBus : public CCObject {
	public:
		EventBus(key_t sem_key, key_t shm_key, size_t items, size_t item_size);
		EventBus(const char *path, size_t items, size_t item_size);
		virtual ~EventBus();
		
		bool	open(bool &force_init);
		void	close();
		bool	post(EventBusData &data);
		bool	get(EventBusData &data, bool skip_to_head=false);
		
		void	get_many(std::vector<EventBusData> &msgs);
		bool	post_many(std::vector<EventBusData> &msgs);
		
		void	dump(void);
		int		wakeup_count;
		bool	valid;
		void*	getExtraHeaderArea(ssize_t bytes);
		
	protected:
		bool	_post(EventBusData &data);
		bool	_get(EventBusData &data);
		bool	_move_to_head(void);
		bool	_wait(void);
		size_t	_align_head(size_t new_head);
		
		void				set_sizes();
		size_t				rb_read(size_t offset, size_t bytes, uint8_t *dst);
		void				rb_write(size_t bytes, uint8_t *src);
		size_t				rb_resync();
		void				make_msg(EventBusMessage_t *msg, size_t length);
		void				construct(key_t sem_key, key_t shm_key, bool init);
		int					notify(void);
		int					wait_for_notification(void);
		int					client_awake(void);
		
		EventBusMessage_t*	msg_for_index(size_t index);
		
		CCSemaphore_t		sem;
		unsigned short		sem_index;
		size_t				num_items,
							data_size,
							item_size,
							buffer_size;
		SemRWLock			*lock;
		CCSharedMem_t		shm;
		EventBusMemoryHeader_t		*header;
		size_t						head;
		uint64_t					last_serial;
		pid_t						pid;
		bool						fixed_width;
};


#endif

