#ifndef _SPSC_QUEUE_H_
#define _SPSC_QUEUE_H_

/* 8KB 레코드 정의 (1024개의 double + timestamp) */
#define DOUBLES_PER_RECORD (1024+1)
#define RECORD_SIZE (DOUBLES_PRE_RECORD * sizeof(double))

#define QUEUE_SIZE  (4096)  /* 큐 크기 (2의 거듭제곱이어야 함) */
#define QUEUE_MASK  (QUEUE_SIZE - 1)

/* 캐시 라인 크기 */
#define CACHE_LINE_SIZE 64

/* 메모리 정렬 매크로 (GCC 4용) */
#define ALIGNED(n) __attribute__((aligned(n)))

/* 캐시 라인 크기 */
#define CACHE_LINE_SIZE 64

/* 메모리 정렬 매크로 (GCC 4용) */
#define ALIGNED(n) __attribute__((aligned(n)))

/* 레코드 구조체 */
typedef struct {
    double data[DOUBLES_PER_RECORD];
} record_t;

/* SPSC 큐 구조체 (Lock-free) */
typedef struct {
    /* Producer와 Consumer가 각각 다른 캐시 라인을 사용하도록 정렬 */
    volatile uint64_t head ALIGNED(CACHE_LINE_SIZE);    /* Producer가 사용 */
    volatile uint64_t tail ALIGNED(CACHE_LINE_SIZE);    /* Consumer가 사용 */
    size_t size;       /* 큐 크기 */
    size_t mask;       /* 비트 마스크 */
    record_t* buffer;  /* 레코드 버퍼 */
} spsc_queue_t;

/* 성능 통계 */
typedef struct {
    uint64_t total_produced;
    uint64_t total_consumed;
    uint64_t max_latency_ns;
    uint64_t min_latency_ns;
    uint64_t total_latency_ns;
    uint64_t queue_full_count;
    uint64_t total_qdepth;
} stats_t;

spsc_queue_t* spsc_queue_create(size_t size);
void spsc_queue_destroy(spsc_queue_t* queue);

/* Producer: 큐에 데이터 추가 (Lock-free) */
static inline int spsc_queue_enq(spsc_queue_t* queue, const record_t* record) {
    const uint64_t head = queue->head;
    const uint64_t tail = queue->tail;  /* volatile 읽기 */
    
    /* 큐가 가득 찬지 확인 */
    if (head - tail >= queue->size) {
        return 0;  /* 큐가 가득 참 */
    }
    
    /* 데이터 복사 */
    memcpy(&queue->buffer[head & queue->mask], record, sizeof(record_t));
    
    /* Memory barrier를 통해 데이터 쓰기 완료 후 head 업데이트 */
    __sync_synchronize();  /* GCC 4의 메모리 배리어 */
    queue->head = head + 1;
    
    return 1;
}

/* Consumer: 큐에서 데이터 제거 (Lock-free) */
static inline int spsc_queue_deq(spsc_queue_t* queue, record_t* record) {
    const uint64_t tail = queue->tail;
    const uint64_t head = queue->head;  /* volatile 읽기 */
    
    /* 큐가 비어있는지 확인 */
    if (tail == head) {
        return 0;  /* 큐가 비어있음 */
    }
    
    /* 데이터 복사 */
    memcpy(record, &queue->buffer[tail & queue->mask], sizeof(record_t));
    
    /* Memory barrier를 통해 데이터 읽기 완료 후 tail 업데이트 */
    __sync_synchronize();  /* GCC 4의 메모리 배리어 */
    queue->tail = tail + 1;
    
    return 1;
}

/* 큐 사용률 확인 */
static inline size_t spsc_queue_size(spsc_queue_t* queue) {
    const uint64_t head = queue->head;
    const uint64_t tail = queue->tail;
    return head - tail;
}


#endif
