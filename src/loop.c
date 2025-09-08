// simple_125us_absolute.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sched.h>
#include <unistd.h>
#include <sys/mman.h>
#include <signal.h>
#include <pthread.h>#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <errno.h>

#define NSEC_PER_SEC 1000000000L
#define PERIOD_NS    125000L  // 125 usec
#define RT_PRIORITY    80           // 1..99, 높을수록 우선순위 높음

static inline void do_work(void) {
}


// 스택 프리폴트(페이지 폴트 방지)
static void prefault_stack(void) {
    const size_t sz = 256 * 1024; // 256KB
    volatile char buf[256 * 1024];
    for (size_t i = 0; i < sz; i += 4096) buf[i] = 0;
}


static inline void add_ns(struct timespec* t, long ns) {
    t->tv_nsec += ns;
    if (t->tv_nsec >= NSEC_PER_SEC) {
        t->tv_nsec -= NSEC_PER_SEC;
        t->tv_sec  += 1;
    }
}


static uint64_t now_ns_realtime(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return 0; // 실패 시 0
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

uint64_t base;
int loop = 0;

uint64_t print_now_ns(void) {
    uint64_t ns = now_ns_realtime();
    if (ns == 0) {
        perror("clock_gettime");
        return;
    }
    printf("%" PRIu64 "\n", (ns - base));
    return ns;
}

int main(void)
{
    struct timespec next;
    base = print_now_ns();

    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        perror("mlockall");
        // 실패해도 계속 진행 가능은 함
    }
    prefault_stack();

    struct sched_param sp = { .sched_priority = RT_PRIORITY };
    if (sched_setscheduler(0, SCHED_FIFO, &sp) != 0) {
        perror("sched_setscheduler (need root/CAP_SYS_NICE)");
    }
    // cpu binding
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        perror("sched_setaffinity");
    }

    clock_gettime(CLOCK_MONOTONIC, &next);
    for (;;) {

        do_work();
        add_ns(&next, PERIOD_NS);
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);
        if  ( (loop++ % 8000) == 0)
        {
            print_now_ns();
        }
    }
    return 0;
}
