#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <machbase_sqlcli.h>

#define SPARAM_MAX_COLUMN   1024

#define ERROR_CHECK_COUNT	100000
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
static char          gTable[64]="TAG";

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
int createTable();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
unsigned long appendClose(SQLHSTMT aStmt);


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

    sprintf(sConnStr,"DSN=%s;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", gTargetIP, gPortNo);
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

void appendTps(SQLHSTMT aStmt)
{
    SQL_APPEND_PARAM *sParam;
    SQLRETURN         sRC;
    unsigned long     i;
    unsigned long     sRecCount = 0;
    char	      sTagName[20];

    struct tm         sTm;
    long              StartTime;
    int               sBreak = 0;
    double            sVal[SPARAM_MAX_COLUMN] = {0, };

    sParam = malloc(sizeof(SQL_APPEND_PARAM) * SPARAM_MAX_COLUMN);
    memset(sParam, 0, sizeof(SQL_APPEND_PARAM)* SPARAM_MAX_COLUMN);

    StartTime = getTimeStamp();
    for( i = 0; (gMaxData == 0) || sBreak == 0; i++ )
    {
        snprintf(sTagName, 20, "PLC1");

        sParam[0].mVar.mLength        = strnlen(sTagName,20);
        sParam[0].mVar.mData          = sTagName;
        sParam[1].mDateTime.mTime     = get_time_us();

        for (int K = 0; K < SPARAM_MAX_COLUMN; K++)
        {
            sParam[K + 2].mDouble = sVal[K];
            sVal[K] += 1.0;
        }

        sRC = SQLAppendDataV2(aStmt, sParam);

        sRecCount++;
        CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);
        if ((gTps != 0) && (sRecCount % 10 == 0))
        {
            long usecperev = 1000000/gTps;
            long sleepus;
            long elapsed = getTimeStamp() - StartTime;
            sleepus = (usecperev * i) - elapsed;
            if (sleepus > 0)
            {
                struct timespec sleept;
                struct timespec leftt;
                sleept.tv_sec = 0;
                sleept.tv_nsec = sleepus * 1000;
                nanosleep(&sleept, &leftt);
            }
        }
        if (sRecCount >= gMaxData)
        {
            goto exit;
        }
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

int appendData(SQLHSTMT aStmt)
{
    appendTps(aStmt);

    return RC_SUCCESS;
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
    time_t      sStartTime, sEndTime;

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
