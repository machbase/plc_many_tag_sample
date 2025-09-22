#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sched.h>
#include <sys/time.h>
#include <machbase_sqlcli.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <errno.h>
#include "spsc_queue.h"

#define NSEC_PER_SEC 1000000000L
#define PERIOD_NS    125000L  // 125 usec
#define RT_PRIORITY    80
#define AFFINITY_CPU   0
#define SPARAM_MAX_COLUMN   (1024+1) /* 1024개의 double + 1개의 timestamp */

#define ERROR_CHECK_COUNT	80000
#define RC_SUCCESS          	0
#define RC_FAILURE          	-1

#define UNUSED(aVar) do { (void)(aVar); } while(0)
#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
            ;                                                   \
        }                                                       \
    }

SQLHENV 	gEnv;
SQLHDBC 	gCon;

static char          gTargetIP[16];
static int           gPortNo=8086;
static unsigned long gMaxData=0;
static long gTps=0;
static char          gTable[64]="plc";

static char *gEnvVarNames[] = {
                          "TEST_MAX_ROWCNT",
                          "TEST_TARGET_EPS",
                          "TEST_PORT_NO",
                          "TEST_SERVER_IP",
                          NULL
};

time_t getTimeStamp();
void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
int connectDB();
void disconnectDB();
int executeDirectSQL(const char *aSQL, int aErrIgnore);
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
unsigned long appendClose(SQLHSTMT aStmt);

/* 글로벌 변수 */
static spsc_queue_t* g_queue;
static stats_t g_stats = {0, 0, 0, ~0ULL, 0, 0, 0};  /* UINT64_MAX 대신 */

static inline void add_ns(struct timespec* t, long ns) {
    t->tv_nsec += ns;
    if (t->tv_nsec >= NSEC_PER_SEC) {
        t->tv_nsec -= NSEC_PER_SEC;
        t->tv_sec  += 1;
    }
}

static inline long long ts_sub(const struct timespec *a, const struct timespec *b)
{
    struct timespec out;
    out.tv_sec  = a->tv_sec  - b->tv_sec;
    long nsec    = a->tv_nsec - b->tv_nsec;
    if (nsec < 0) { out.tv_sec -= 1; nsec += NSEC_PER_SEC; }
    out.tv_nsec = nsec;

    return out.tv_sec * NSEC_PER_SEC + out.tv_nsec;
}

static uint64_t now_ns_realtime(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return 0; // 실패 시 0
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

time_t getTimeStamp()
{
    struct timeval sTimeVal;
    int            sRet;

    sRet = gettimeofday(&sTimeVal, NULL);

    if (sRet == 0)
    {
        return (time_t)(sTimeVal.tv_sec * 1000000 + sTimeVal.tv_usec);
    }
    else
    {
        return 0;
    }
}

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if( sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606 )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void appendDumpError(SQLHSTMT    aStmt,
                 SQLINTEGER  aErrorCode,
                 SQLPOINTER  aErrorMessage,
                 SQLLEN      aErrorBufLen,
                 SQLPOINTER  aRowBuf,
                 SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}


int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS )
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS )
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"DSN=%s;UID=SYS;PWD=MANAGER;CONNTYPE=2;PORT_NO=%d", gTargetIP, gPortNo);
    printf("Connecting %s...\n", sConnStr);
    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = gTable;

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

long long get_time_us()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    return (long long)ts.tv_sec * 1000000000L + ts.tv_nsec;
}

void queueConsume(void *aArg)
{
    char	      sTagName[20];
    record_t      sVal;
    SQL_APPEND_PARAM *sParam;
    SQLRETURN         sRC;
    SQLHSTMT          sStmt = (SQLHSTMT)aArg;
    uint64_t i = 0;

    snprintf(sTagName, 20, "PLC1");

    sParam = malloc(sizeof(SQL_APPEND_PARAM) * (SPARAM_MAX_COLUMN + 2));
    memset(sParam, 0, sizeof(SQL_APPEND_PARAM)* (SPARAM_MAX_COLUMN + 2));
    sParam[0].mVar.mLength        = strnlen(sTagName,20);
    sParam[0].mVar.mData          = sTagName;
    for( i = 0; g_stats.total_consumed < gMaxData; i++ )
    {
    retry:
        if (spsc_queue_deq(g_queue, &sVal)) {
            g_stats.total_consumed++;
        }
        else {
            usleep(10); // 큐가 비어있으면 잠시 대기
            goto retry;
        }
        // Copy timestamp from data[]
        memcpy(&sParam[1].mDateTime.mTime, &sVal.data[0], sizeof(double));
        for (int K = 0; K < SPARAM_MAX_COLUMN; K++)
        {
            sParam[K + 1].mDouble = sVal.data[K];
        }
        sRC= SQLAppendDataV2(sStmt, sParam);
        CHECK_APPEND_RESULT(sRC, gEnv, gCon, sStmt);
    }
    return;
}

void appendqueue(SQLHSTMT aStmt)
{

    unsigned long     i;

    struct tm         sTm;
    long              StartTime;
    struct            timespec  next;
    long long         maxgap = 0;
    long long         current_us;
    record_t          sVal;

    StartTime = getTimeStamp();
    clock_gettime(CLOCK_MONOTONIC, &next);
    for( i = 0; g_stats.total_produced < gMaxData ; i++ )
    {
        current_us = get_time_us();
        memcpy(&sVal.data[0], &current_us, sizeof(double));

        for (int K = 1; K < SPARAM_MAX_COLUMN; K++)
        {
             sVal.data[K] += 1.123412341234;
        }

        {
            record_t sRecord;
            struct timespec  before, after;
            long long gap;

            clock_gettime(CLOCK_MONOTONIC, &before);            
        retry:
            if (spsc_queue_enq(g_queue, &sVal)) {
                g_stats.total_produced++;
                g_stats.total_qdepth = spsc_queue_size(g_queue);
            } 
            else {
                // 큐가 가득 찼을 때 처리 (예: 대기 또는 건너뜀)
                    usleep(10); // 1 마이크로초 대기
                    g_stats.queue_full_count++;
                    goto retry;
            }
            clock_gettime(CLOCK_MONOTONIC, &after);
            gap = ts_sub(&after, &before);
            g_stats.total_latency_ns += gap;
            if (gap > g_stats.max_latency_ns) {
                g_stats.max_latency_ns = gap;
            }
            if (gap < g_stats.min_latency_ns) {
                g_stats.min_latency_ns = gap;
            }
            if (gap > PERIOD_NS)
            {
                if (gap > maxgap)
                {
                    maxgap = gap;
                }
                fprintf(stderr, "gap(%dth, max=%lld) = %lld\n", i, maxgap, gap);
            }
        }
        add_ns(&next, PERIOD_NS);
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);
    }

/*
   printf("====================================================\n");
   printf("total time : %ld sec\n", sEndTime - sStartTime);
   printf("average tps : %f \n", ((float)gMaxData/(sEndTime - sStartTime)));
   printf("====================================================\n");
*/
exit:
    return;
}

/* 통계 출력 */
void print_statistics(void) {
    double sMaxData = (double)gMaxData;
    printf("\n=== Performance Statistics ===\n");
    printf("Queue Full Count: %llu\n", (unsigned long long)g_stats.queue_full_count);
    
    if (gMaxData > 0) {
        printf("Max Latency: %llu ns (%.2f μs)\n", 
               (unsigned long long)g_stats.max_latency_ns, 
               g_stats.max_latency_ns / 1000.0);
        printf("Min Latency: %llu ns (%.2f μs)\n", 
               (unsigned long long)g_stats.min_latency_ns, 
               g_stats.min_latency_ns / 1000.0);
        printf("Avg Latency: %g ns (%g μs)\n", 
               (double)g_stats.total_latency_ns / sMaxData,
               (double)g_stats.total_latency_ns * 1000 / sMaxData );
        printf("Avg Queue Depth: %g\n", 
               (double)g_stats.total_qdepth / sMaxData); 
    }
}

int appendData(SQLHSTMT aStmt)
{
    int ret;
    pthread_t      consumer_tid;
    g_queue = spsc_queue_create(QUEUE_SIZE);
    if (!g_queue) {
        printf("Failed to create SPSC queue\n");
        return 1;
    }
    ret = pthread_create(&consumer_tid, NULL, queueConsume, (void *)aStmt);
    if (ret != 0) {
        printf("Failed to create consumer thread: %s\n", strerror(ret));
    }
    appendqueue(aStmt);
    pthread_join(consumer_tid, NULL);
    spsc_queue_destroy(g_queue);
    print_statistics();
    return 0;
    
error_return:
    if (consumer_tid)
        pthread_join(consumer_tid, NULL);
    if (g_queue)
        spsc_queue_destroy(g_queue);
    return 1;
}

unsigned long appendClose(SQLHSTMT aStmt)
{
    unsigned long sSuccessCount = 0;
    unsigned long sFailureCount = 0;

    if( SQLAppendClose(aStmt, (SQLBIGINT *)&sSuccessCount, (SQLBIGINT *)&sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int SetGlobalVariables()
{
    int i = 0;
    char        *sEnvVar;

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gMaxData = atoll(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gTps    = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gPortNo  = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    strncpy(gTargetIP, sEnvVar, sizeof(gTargetIP));
    return 0;
error:
    printf("Environment variable %s was not set!\n", gEnvVarNames[i]);
    return -1;
}


int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;

    unsigned long  sCount=0;
    time_t         sStartTime, sEndTime;
    int            ret;

    struct sched_param sp = { .sched_priority = RT_PRIORITY };
    if (sched_setscheduler(0, SCHED_FIFO, &sp) != 0) {
        perror("sched_setscheduler (need root/CAP_SYS_NICE)");
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(AFFINITY_CPU, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        perror("sched_setaffinity");
    }

    if (SetGlobalVariables() != 0)
    {
        exit(-1);
    }
    srand(time(NULL));

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }
  
    sStartTime = getTimeStamp();
    appendData(sStmt);
    sEndTime = getTimeStamp();

    sCount = appendClose(sStmt);
    if( sCount > 0 )
    {
        printf("appendClose success\n");
        printf("timegap = %ld microseconds for %ld records\n", sEndTime - sStartTime, sCount);
        printf("%.2f records/second\n",  ((double)sCount/(double)(sEndTime - sStartTime))*1000000);
    }
    else
    {
        printf("appendClose failure\n");
    }

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

    disconnectDB();
    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }

    return RC_FAILURE;
}
