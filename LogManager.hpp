#include <cstring>


#include "LogRecord.hpp"
#include "checkpoint.hpp"
#include "backing_store.hpp"
#include "swap_space.hpp"

#define LOG_BUFFER_SIZE  4096
#define CHECKPOINT_GRANULARITY 8
#define PERSISTENCE_GRANULARITY 16

class LogManager {
public:
    LogManager(swap_space *ss, uint64_t persistence_granularity = PERSISTENCE_GRANULARITY, 
            uint64_t checkpoint_granularity = CHECKPOINT_GRANULARITY, 
            std::string logDir = "log") 
            : checkpointGranularity_(checkpoint_granularity),
            persistenceGranularity_(persistence_granularity), 
            ss_(ss) {
        logBuf_ = new char[LOG_BUFFER_SIZE];
        logBufPtr_ = logBuf_;
        //checkpoint_ = new checkPoint();
        log_ = new LogFileBackingStore(logDir);
    }

    ~LogManager() {
        delete[] logBuf_;
        logBuf_ = nullptr;
        //delete checkpoint_;
    }

    void flushLogBuf() {
        log_->appendData(logBuf_, bufLen);
        bufLen = 0;
        //
        flushLsn_ = nextLsn_;
        logBufPtr_ = logBuf_;
        flushTimes_++;
    }

    long long getNextLsn() {
        return nextLsn_++;
    }

    // return true, need to do checkpoint;
    // return false, no need to do checkpoint;
    bool appendLogRec(LogRecord &logRec) {
        if (recNum_ > MAX_LOG_RECORD_NUM || bufLen + logRec.getLen() >= LOG_BUFFER_SIZE) {  //to do, config variable.
            if (bufLen + logRec.getLen() >= LOG_BUFFER_SIZE) {
                debug(std::cout << "flushLogBuf triggerred by bufLen + \
                        logRec.getLen() >= LOG_BUFFER_SIZE" << std::endl);
            } else {
                debug(std::cout << "flushLogBuf triggerred recNum_ > MAX_LOG_RECORD_NUM " << std::endl);
            }
            flushLogBuf();
        }
        int len = logRec.getLen();
        logRec.serialize(logBufPtr_, len);
        logBufPtr_ += len;
        bufLen += len;
        if (flushTimes_ >= persistenceGranularity_) {
            debug(std::cout << "Need to do check pointing" << std::endl);
            return true;
        }
        return false;
    }

    void doCheckPoint(u_int64_t rootId) {
        u_int64_t curCheckpointLsn = getNextLsn();
        LogRecordType tp = LogRecordType::CHECKOUT_POINT;
        flushLogBuf();
        debug(std::cout << "flush all modified blocks in memory" << std::endl);
        // flush all modified blocks in memory
        ss_->flushAllModifiedPagesIntoDisk();
        u_int64_t txtId = getNextTxtId();
        LogRecord checkpointLogRec(txtId, curCheckpointLsn, NULL_LSN,
                tp, rootId);
        appendLogRec(checkpointLogRec);
        flushTimes_ = 0;
        debug(std::cout << "start to parse log" << std::endl);
        //parseLog();
    }

    u_int64_t getNextTxtId() {
        return txtId_++;
    }

    void parseLog() {
        int len = 0;
        std::ifstream* logStream = log_->get(len);
        debug(std::cout << "On disk log length:" << len << std::endl);
        // Create a char* buffer to hold the content
        char* buffer = new char[len];

        // Read the content of the file into the buffer
        logStream->read(buffer, len);
        char * cur = buffer;
        for (int i = 0; i < len; ) {
            LogRecord lr(cur, len - i);
            std::string tmp = lr.debugDump();
            cur += lr.getLen();
            i += lr.getLen();
            debug(std::cout << tmp << std::endl);
        }
    }
/*
  bool checkpointing() {
    long long curCheckpointLsn = logManager_->getNextLsn();
    writeCheckPointBeginLog();
    flushLogRecordsInMemory();
    flushAllModifiedBlocksInMemory();
    curCheckpointLsn = logManager_->getNextLsn();
    writeCheckPointEndLog(curCheckpointLsn);
    logManager_->trancateLogFile(lastCheckpointLsn_);
    lastCheckpointLsn_ = curCheckpointLsn;
  }
  
  bool flushLogRecordsInMemory() {
    logManager_->flushLogBuf();
  }
  bool flushAllModifiedBlocksInMemory();
  bool writeCheckPointBeginLog(long long lsn);
  bool writeCheckPointEndLog(long long lsn);
private:
  long long lastCheckpointLsn_;
  LogManager *logManager_;
};
*/
private:
  LogFileBackingStore *log_;
  swap_space *ss_;
  char *logBuf_;
  char *logBufPtr_;
  int bufLen = 0;
  u_int64_t nextLsn_ = 0;
  u_int64_t flushLsn_ = 0;
  u_int64_t txtId_ = 0;
  int recNum_ = 0;
  //checkPoint *checkpoint_;
  int checkpointGranularity_;
  int persistenceGranularity_;
  int flushTimes_ = 0;
  long long lastCheckpointLsn_ = 0;
};