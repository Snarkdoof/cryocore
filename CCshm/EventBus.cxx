/*	EventBus.cxx
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#include "EventBus.h"

EventBus::EventBus(const char *path, size_t num_items, size_t item_size) :
	CCObject(), num_items(num_items), item_size(item_size) {
	pid		= getpid();
	sem.key = ftok(path, 1);
	shm.key = ftok(path, 2);
}


EventBus::EventBus(key_t sem_key, key_t shm_key, size_t num_items, size_t item_size) : 
	CCObject(), num_items(num_items), item_size(item_size) {
	pid		= getpid();
	sem.key = sem_key;
	shm.key = shm_key;
}


EventBus::~EventBus() {
	close();
}


bool	EventBus::open(bool &force_init) {
	if (!CCSemaphoreGet(sem, 4, force_init))
		return false;
	/*	The EventBus uses 4 semaphores:
		0: A semaphore used for notifications
		1-3: Semaphores used for read-write lock
	*/
	sem_index	= 0;
	lock		= new SemRWLock(sem.sem, sem_index+1);
	set_sizes();
	bool	use_existing_sizes = false;
	
	if (!CCMapSharedMemory(shm, use_existing_sizes, force_init, buffer_size, 4096))
		return false;
	header	= (EventBusMemoryHeader_t*)shm.ptr;
	//printf("Shared memory pointer: %p\n", shm.ptr);
	//	We align the start with a page boundary (assuming 4k pages),
	//	in case we want to get fancy with mapping the ring buffer.
	if (force_init) {
		printf("Initializing\n");
        init_locks();
		memset(shm.ptr, 0, shm.size);
		header->num_items = num_items;
		header->item_size = item_size;
	}
	else if (use_existing_sizes) {
		num_items = header->num_items;
		item_size = header->item_size;
		set_sizes();
	}
	
	//	Read initial state of ring buffer.
	lock->readLock();
	head		= header->head;
	last_serial	= header->serial;
	lock->unlock();
	valid 		= true;
	return true;
}


void	EventBus::close() {
	CCUnmapSharedMemory(shm);
	if (lock)
		lock->release();
	lock 		= 0;
	header 		= 0;
	head		= 0;
	last_serial = 0;
	valid 		= false;
}


void	EventBus::set_sizes() {
	if (num_items == 0) {
		fixed_width	= false;
		data_size	= item_size;
		buffer_size = data_size;
	}
	else {
		fixed_width	= true;
		data_size	= item_size + sizeof(EventBusMessage_t);
		buffer_size = data_size * num_items;
	}
}


bool	EventBus::post(EventBusData &data) {
	lock->writeLock();
	bool posted = _post(data);
	//printf("Posting; serial %llu head %zu\n", header->serial, header->head);
	lock->unlock();
	if (posted)
		notify();
	return posted;
}


bool	EventBus::post_many(std::vector<EventBusData> &msgs) {
	bool	any_posted = false;
	lock->writeLock();
	for (size_t i=0;i<msgs.size();i++)
		any_posted |= _post(msgs[i]);
	//printf("Posting; serial %llu head %zu\n", header->serial, header->head);
	lock->unlock();
	if (any_posted)
		notify();
	return any_posted;
}


bool	EventBus::_post(EventBusData &data) {
	if ((fixed_width && data.length > item_size) ||
		(!fixed_width && (data.length + sizeof(EventBusMessage_t)) > buffer_size)) {
		if (data.free_after) {
			free(data.data);
			data.data = 0;
		}
		return false;
	}
	header->serial++;
	if (fixed_width) {
		header->head	= (header->head + 1) % num_items;
		EventBusMessage_t	*msg = msg_for_index(header->head);
		make_msg(msg, data.length);
		memcpy(msg->data, data.data, data.length);
	}
	else {
		EventBusMessage_t	msg = {};
		make_msg(&msg, data.length);
		rb_write(sizeof(EventBusMessage_t), (uint8_t*)&msg);
		rb_write(data.length, (uint8_t*)data.data);
		size_t align = 8 - (header->head % 8);
		if (align > 0) {
			header->head += align;
			header->head %= buffer_size;
		}
	}
	if (data.free_after) {
		free(data.data);
		data.data = 0;
	}
	return true;
}


//	Assumes read or write lock on shared mem is set
void	EventBus::make_msg(EventBusMessage_t *msg, size_t length) {
	msg->magic		= kEventBusMagic;
	msg->serial 	= header->serial;
	msg->timestamp	= double_time();
	msg->length 	= length;
	msg->pid		= pid;
}


size_t	EventBus::rb_read(size_t offset, size_t bytes, uint8_t *dst) {
	size_t	start = offset;
	
	if (start + bytes <= buffer_size) {
		memcpy(dst, shm.data + start, bytes);
	}
	else {
		size_t	chunk1 = (start + bytes) - buffer_size,
				chunk0 = bytes - chunk1;
		if (chunk0 > 0)
			memcpy(dst, shm.data + start, chunk0);
		if (chunk1 > 0)
			memcpy(dst+chunk0, shm.data, chunk1);
		//printf("Splitting read: %zu %zu : %zu %zu\n", offset, bytes, chunk0, chunk1);
	}
	start += bytes;
	start %= buffer_size;
	return start;
}


void	EventBus::rb_write(size_t bytes, uint8_t *src) {
	//	We always write starting at header->head.
	size_t	start = header->head;
	if (start + bytes <= buffer_size) {
		//	Easy, just copy everything in one go
		memcpy(shm.data + start, src, bytes);
	}
	else {
		//	Must split the copy. We'd better hope the math is solid and that we never end up with negative values here.
		size_t	chunk1 = (start + bytes) - buffer_size,
				chunk0 = bytes - chunk1;
		if (chunk0 > 0)
			memcpy(shm.data + start, src, chunk0);
		if (chunk1 > 0)
			memcpy(shm.data, src+chunk0, chunk1);
		//printf("Splitting write: %zu %zu : %zu %zu\n", start, bytes, chunk0, chunk1);
	}
	header->head += bytes;
	header->head %= buffer_size;
}


bool	EventBus::get(EventBusData &data, bool skip_to_head) {
	bool	result = false,
			error = false;

	while (!result) {
		lock->readLock();
		error = _wait();
		//	We won't have readLock if there was an error (most likely: sig int).
		//	The client takes over responsibility of releasing readLock if we return without an error.
		if (error)
			break;
		if (skip_to_head) {
			//	Move to head skips our head to the last message that we can deliver,
			//	ie any message that is valid, and is not from ourselves
			if (_move_to_head())
				result = _get(data);
		}
		else
			result = _get(data);
		lock->unlock();
	}
	return result;
}


void	EventBus::get_many(std::vector<EventBusData> &msgs) {
	size_t			initial_size = msgs.size();
	while (msgs.size() == initial_size) {
		lock->readLock();
		bool error = _wait();
		if (error)
			break;
		
		while (header->serial != last_serial) {
			EventBusData data = {};
			size_t			old_head = head;
			bool got_msg = _get(data);
			if (got_msg)
			 	msgs.push_back(data);
			if (old_head == head) {
				//	We are not making progress, and rb_resync didn't find anything useful.
				//	Break out, release the lock and return any messages we got (or keep spinning
				//	if we didn't get any)
				break;
			}
		}
		lock->unlock();
	}
}


bool	EventBus::_wait() {
	bool	error = false;
	//	Any new data?
	while (header->serial == last_serial) {
		lock->unlock();
		if (wait_for_notification() != 0) {
			error = true;
			break;
		}
		lock->readLock();
	}
	return error;
}


bool	EventBus::_get(EventBusData &data) {
	bool	got_msg = false;
	if (fixed_width) {
		//printf("Checking; serial %llu head %zu last sn: %llu\n", header->serial, header->head, last_serial);
		//	For now, we have a simple life: We can meander along the list
		//	as we please, since noone will be updating it while we have our read lock.
		//	We start reading at our cached head. If we fail to stay current with the buffer
		//	(global head overtakes our head), we don't do any special handling of that.	
		EventBusMessage_t	*msg = msg_for_index(head);
		//	TODO: Find a clever way to handle the unlikely case of serial overflow.
		if (msg->serial > last_serial) {
			last_serial	= msg->serial;
			if (msg->pid != pid && msg->length <= item_size) {
				data.data = malloc(msg->length+1); // We add a byte here to allow seamless transfer to python bytearrays
				data.length = msg->length;
				data.free_after = true;
				memcpy(data.data, msg->data, msg->length);
				got_msg = true;
			}
		}
		head		= (head + 1) % num_items;
	}
	else {
		EventBusMessage_t	msg = {};
		int					head_reset = 0;
		do {
			size_t				new_head = head;
			new_head = rb_read(new_head, sizeof(EventBusMessage_t), (uint8_t*)&msg);
			//	Validate magic, serial, etc
			if (msg.magic == kEventBusMagic) {
				if (msg.serial > last_serial) {
					last_serial = msg.serial;
					if (msg.pid != pid && msg.length < buffer_size) {
						data.data = malloc(msg.length+1); // We add a byte here to allow seamless transfer to python bytearrays
						data.length = msg.length;
						data.free_after	= true;
						rb_read(new_head, msg.length, (uint8_t*)data.data);
						got_msg = true;
					}
				}
				//	Magic must match for us to trust this message.
				new_head	+= msg.length;
				head		= _align_head(new_head);
				break;
			}
			else {
				//	We've lost sync with the current head. There are two approaches
				//	we can take now: Resync by setting our head to header->head (efficient but lossy), or
				//	scan through the region looking for the first valid msg magic.
				printf("Lost sync, reset head\n");
				head		= rb_resync();
				head_reset++;
				//head 		= header->head;
				//last_serial = header->serial;
			}
		} while (head_reset < 2);
	}
	return got_msg;
}



size_t	EventBus::_align_head(size_t new_head) {
	size_t align = 8 - (new_head % 8);
	if (align > 0)
		new_head += align;
	new_head %= buffer_size;
	return new_head;
}


bool	EventBus::_move_to_head(void) {
	if (fixed_width) {
		printf("Currently unimplemented\n");
	}
	else {
		/*	The idea here is to skip through the messages in the buffer,
			saving the head position of any message that passes our
			"valid message" test. 
		*/
		ssize_t		valid_head = -1,
					msg_head,
					cur_head = head,
					head_reset = 0;
		uint64_t	valid_serial = 0,
					cur_serial = last_serial;
		
		while (header->serial != cur_serial && head_reset < 2) {
			EventBusMessage_t	msg = {};
			size_t				new_head = cur_head;
			msg_head = new_head;
			new_head = rb_read(new_head, sizeof(EventBusMessage_t), (uint8_t*)&msg);
			if (msg.magic == kEventBusMagic) {
				if (msg.serial > cur_serial) {
					cur_serial = msg.serial;
					if (msg.pid != pid && msg.length < buffer_size) {
						valid_head = msg_head;
						valid_serial = msg.serial - 1;
					}
				}
				new_head	+= msg.length;
				cur_head	= _align_head(new_head);
			}
			else {
				printf("Resync head in move_to_head\n");
				cur_head	= rb_resync();
				head_reset++;
			}
		}
		if (valid_head) {
			//	Adjust head to point at start of the last valid message
			head = valid_head;
			//	The serial is one less than the serial of the message we are about to receive
			last_serial = valid_serial;
			return true;
		}
		else {
			//	Head may have changed, but we didn't find any messages that were to us.
			head = cur_head;
			last_serial = cur_serial;
		}
	}
	return false;
}


size_t	EventBus::rb_resync() {
	size_t	cur_head = head + 8;
	
	if ((head % 8) != 0) {
		printf("ERROR: Head is not a multiple of 8\n");
		return header->head;
	}
	
	cur_head %= buffer_size;
	while (cur_head != head) {
		uint64_t	*magic = (uint64_t*)(shm.data + cur_head);
		if (*magic == kEventBusMagic) {
			return cur_head;
		}
		cur_head	+= 8;
		cur_head	%= buffer_size;
	}
	return header->head;
}


int		EventBus::notify() {
	semun 	c;
	c.val = 0;
	//	First set value to zero, unblocking all
	int result = 0;
	do { result = semctl(sem.sem, sem_index, SETVAL, c);
	} while (result != 0 && errno == EINTR);
	return result;
}


int		EventBus::wait_for_notification() {
	//printf("Current value: %d\n", semctl(sem, sem_index, GETVAL));
	sembuf op = { sem_index, 0, 0 };
	int result = 0;
	//printf("Wait for notification..\n");
	do { result = semop(sem.sem, &op, 1);
	} while (result != 0 && errno == EINTR);
	result = client_awake();
	return result;
}


int		EventBus::client_awake() {
	//	Up the semaphore to make clients block next time they call wait.
	//	All clients do this after receiving a wakeup. Ideally the sender
	//	could do the up, however that led to a nasty case of having to
	//	yield between semctl and semop in order for clients to actually
	//	wake up. This is slightly more robust, but there may still be
	//	race conditions for multiple listeners depending on how the kernel
	//	implements the wakeup-on-zero semaphore.
	//	In fact, I suspect I'm seeing the results of that race condition here,
	//	at least on OS X. The wakeups may not be as robust as I would like.
	sembuf op = { sem_index, 1, 0 };
	int result = 0;
	do { result = semop(sem.sem, &op, 1);
	} while (result != 0 && errno == EINTR);
	wakeup_count++;
	return result;
}


EventBusMessage_t*	EventBus::msg_for_index(size_t index) {
	return (EventBusMessage_t*)(shm.data + (data_size * index));
}


void	EventBus::dump() {
	printf("%d has value %d\n", sem_index, semctl(sem.sem, sem_index, GETVAL));
	lock->dump();
}


void*	EventBus::getExtraHeaderArea(ssize_t bytes) {
	const ssize_t	header_size = 4096;
	if (header && (header_size - bytes - sizeof(EventBusMemoryHeader_t)) > 0)
		return (void*)&header->extra[0];
	return 0;
}
