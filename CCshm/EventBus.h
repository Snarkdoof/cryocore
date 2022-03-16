/*	EventBus.h
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#ifndef EventBus_h
	#define EventBus_h

#include "SemRWLock.h"
#include "SharedMem.h"
#include <vector>
#include <map>
#include <pthread.h>

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

class EventBusManager : public CCObject {
    public:
        EventBusManager() : CCObject() {
            pthread_mutex_init(&lock, 0);
            pthread_cond_init(&cond, 0);
            pthread_create(&_tid, 0, EventBusManager::post_thread, this);
        };
        virtual ~EventBusManager() {
            pthread_mutex_lock(&lock);
            for (auto it=pending.begin();it!=pending.end();++it) {
                std::vector<struct EventBusData> &items = it->second;
                it->first->release();
                clear_pending(it->second);
            }
            pending.clear();
            _stop = true;
            pthread_cond_broadcast(&cond);
            pthread_mutex_unlock(&lock);
            pthread_join(_tid, 0);
            pthread_mutex_destroy(&lock);
            pthread_cond_destroy(&cond);
        };
    
        void clear_pending(std::vector<struct EventBusData> &items) {
            for (size_t i=0;i<items.size();i++) {
                if (items[i].free_after)
                    free(items[i].data);
            }
            items.clear();
        };
        
        void add_bus(EventBus *bus) {
            pthread_mutex_lock(&lock);
            auto it = pending.find(bus);
            if (it == pending.end()) {
                bus->retain();
                pending[bus] = std::vector<struct EventBusData>();
            }
            pthread_mutex_unlock(&lock);
        };
    
        void remove_bus(EventBus *bus) {
            pthread_mutex_lock(&lock);
            auto it = pending.find(bus);
            if (it != pending.end()) {
                it->first->release();
                clear_pending(it->second);
                pending.erase(it);
            }
            pthread_mutex_unlock(&lock);
        };
        
        void post(EventBus *bus, EventBusData &ed) {
            pthread_mutex_lock(&lock);
            pending[bus].push_back(ed);
            pending_count++;
            pthread_cond_broadcast(&cond);
            pthread_mutex_unlock(&lock);
        };
    
        static void* post_thread(void *arg) {
            EventBusManager *self = (EventBusManager*)arg;
            self->_post_thread();
            return 0;
        };
    
        void _post_thread() {
            std::map<EventBus*, std::vector<struct EventBusData>> to_post;
            while (!_stop) {
                pthread_mutex_lock(&lock);
                while (!_stop && pending_count == 0)
                    pthread_cond_wait(&cond, &lock);
                pending_count = 0;
                to_post = pending;
                //  Retain event buses and clear pending
                for (auto it=pending.begin();it!=pending.end();++it) {
                    it->first->retain();
                    it->second.clear();
                }
                pthread_mutex_unlock(&lock);
                for (auto it=to_post.begin();it!=to_post.end();++it) {
                    if (it->second.size() > 0) {
                        it->first->post_many(it->second);
                    }
                    it->first->release();
                }
            }
        };
    
    protected:
        pthread_mutex_t lock;
        pthread_cond_t cond;
        std::map<EventBus*, std::vector<struct EventBusData>> pending;
        size_t pending_count;
        bool _stop;
        pthread_t _tid;
};



#endif

