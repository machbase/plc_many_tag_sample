
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include "spsc_queue.h"

/* GCC 4용 컴파일러 내장 함수 사용 */
#ifndef __GNUC__
#error "This code requires GCC compiler"
#endif


/* 메모리 정렬된 할당 (GCC 4용) */
static void* aligned_malloc(size_t alignment, size_t size) {
    void *ptr;
    int ret = posix_memalign(&ptr, alignment, size);
    if (ret != 0) {
        return NULL;
    }
    return ptr;
}

/* SPSC 큐 초기화 */
spsc_queue_t* spsc_queue_create(size_t size) {
    if (size == 0 || (size & (size - 1)) != 0) {
        printf("Queue size must be power of 2\n");
        return NULL;
    }
    
    spsc_queue_t* queue = (spsc_queue_t*)aligned_malloc(CACHE_LINE_SIZE, sizeof(spsc_queue_t));
    if (!queue) {
        perror("Failed to allocate queue");
        return NULL;
    }
    
    size_t buffer_size = size * sizeof(record_t);
    queue->buffer = (record_t*)aligned_malloc(CACHE_LINE_SIZE, buffer_size);
        if (!queue->buffer) {
            free(queue);
            return NULL;
        }
    memset(queue->buffer, 0,  buffer_size);
    queue->head = 0;
    queue->tail = 0;
    queue->size = size;
    queue->mask = size - 1;
    
    return queue;
}

/* SPSC 큐 정리 */
void spsc_queue_destroy(spsc_queue_t* queue) {
    if (queue) {
        free(queue->buffer);
        free(queue);
    }
}
