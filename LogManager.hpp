#include <cstring>

#include "LogRecord.hpp"
#include "backing_store.hpp"
#include "swap_space.hpp"

#define LOG_BUFFER_SIZE 8192
#define CHECKPOINT_GRANULARITY 8
#define PERSISTENCE_GRANULARITY 16
#define MAX_LOG_RECORD_SIZE 4096

class LogManager {
public:
    LogManager(swap_space *ss, uint64_t persistence_granularity = PERSISTENCE_GRANULARITY, 
            uint64_t checkpoint_granularity = CHECKPOINT_GRANULARITY, 
            std::string logDir = "tmpdir")
            : checkpointGranularity_(checkpoint_granularity),
            persistenceGranularity_(persistence_granularity), 
            ss_(ss) {
        logBuf_ = new char[LOG_BUFFER_SIZE];
        logBufPtr_ = logBuf_;
        //checkpoint_ = new checkPoint();
        log_ = new LogFileBackingStore(logDir + "/log");
        checkpointNodesInfoFile = logDir + "/checkpointAllNodesInfo.bin";
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
        if (recNum_ >= persistenceGranularity_ || bufLen + logRec.getLen() >= LOG_BUFFER_SIZE) {  //to do, config variable.
            if (bufLen + logRec.getLen() >= LOG_BUFFER_SIZE) {
                debug(std::cout << "flushLogBuf triggerred by bufLen + \
                        logRec.getLen() >= LOG_BUFFER_SIZE" << std::endl);
            } else {
                debug(std::cout << "flushLogBuf triggerred recNum_ > persistenceGranularity_ " << std::endl);
            }
            flushLogBuf();
            recNum_ = 0;
        }
        int len = logRec.getLen();
        logRec.serialize(logBufPtr_, len);
        logBufPtr_ += len;
        bufLen += len;
        if (flushTimes_ >= checkpointGranularity_) {
            debug(std::cout << "Need to do check pointing" << std::endl);
            return true;
        }
        recNum_++;
        return false;
    }

    void doCheckPoint(u_int64_t rootId, u_int64_t rootVersion,
        std::vector<std::pair<u_int64_t, u_int64_t>> &idAndVers) {
        u_int64_t curCheckpointLsn = getNextLsn();
        LogRecordType tp = LogRecordType::CHECKOUT_POINT;
        flushLogBuf();
        debug(std::cout << "flush all modified blocks in memory" << std::endl);
        // flush all modified blocks in memory
        ss_->flushAllModifiedPagesIntoDisk();
        u_int64_t txtId = getNextTxtId();

        LogRecord checkpointLogRec(txtId, curCheckpointLsn, NULL_LSN,
                tp, rootId, rootVersion);
        int logLen = 0;
        log_->get(logLen);
        appendLogRec(checkpointLogRec);
        flushLogBuf();
        log_->truncateLogFile(logLen);
        debug(std::cout << "root id: " << rootId << " version:" << rootVersion);
        flushTimes_ = 0;
        debug(std::cout << "start to parse log" << std::endl);
        //parseLog();
    }

    void saveAllNodesInfo(std::vector<std::pair<u_int64_t, u_int64_t>> &idAndVers) {
        // Open a file for writing
        std::ofstream outputFile(checkpointNodesInfoFile);

        // Check if the file is opened successfully
        assert(outputFile.is_open());

        for (const auto& pair : idAndVers) {
            outputFile << pair.first << ' ' << pair.second << '\n';
        }
        // Close the file
        outputFile.close();
        // flush file to disk.
        outputFile.flush();
    }

    void getALlNodesInfo(std::unordered_map<uint64_t, uint64_t> &objsMap) {
        std::ifstream inputFile(checkpointNodesInfoFile);
        assert(inputFile.is_open());
        // Create a vector to store the parsed data
        std::vector<std::pair<uint64_t, uint64_t>> idAndVersParsed;
        // Read data from the file and parse it
        uint64_t id, vers;
        while (inputFile >> id >> vers) {
            objsMap[id] = vers;
        }
        inputFile.close();
    }

    u_int64_t getNextTxtId() {
        return txtId_++;
    }

    bool getInfoForRecovery(uint64_t &rootId, u_int64_t &rootVer) {
        int len = 0;
        std::ifstream* logStream = log_->get(len);
        debug(std::cout << "On disk log length:" << len << std::endl);

        // Create a char* buffer to hold the content
        bool flag = false;
        char* buffer = new char[LOG_BUFFER_SIZE];
        int readLen = 0;
        int i = LOG_BUFFER_SIZE;
        while (readLen < len) {
            // Read the content of the file into the buffer and find the latest checkpoint log record
            logStream->read(buffer + LOG_BUFFER_SIZE - i, i);
            char * cur = buffer;
            i = 0;
            while (i + MAX_LOG_RECORD_SIZE <= LOG_BUFFER_SIZE && readLen + i + LOG_RECORD_HEAD_LEN <= len) {
                LogRecord lr(cur, MAX_LOG_RECORD_SIZE);
                //lr.debugDump();

                cur += lr.getLen();
                i += lr.getLen();

                if (lr.getLogRecType() != LogRecordType::CHECKOUT_POINT) {
                    redoLog_.push_back(lr);
                } else {
                    //debug(std::cout << "add checkpoint into log" << std::endl);
                    //lr.debugDump();
                    // clear redoLog_ vector to store the operations after the newer checkpoint
                    redoLog_.clear();
                    // The first log record is the latest checkpoint.
                    redoLog_.push_back(lr);
                    // store the root id and version in checkpoint log record
                    rootId = lr.getPageId();
                    rootVer = lr.getKey();
                    flag = true;
                }
                //debug(std::cout << "redoLog_.size():" << redoLog_.size() << std::endl);
            }
            memcpy(buffer, cur, LOG_BUFFER_SIZE - i);
            readLen += i;
            //std::cout << "readLen" << readLen << std::endl;
        }
        delete[] buffer;
        debug(std::cout << "redoLog_.size():" << redoLog_.size() << std::endl);
        debug(std::cout << "rootId:" << rootId << " rootVer:" << rootVer << std::endl);
        return flag;
    }

    int getRedoLogRecordNumber(void) {
        return redoLog_.size();
    }

    // i is the index for redoLog_
    // return True if get suceessfully redoLog_[i]
    // return False if fail to get redoLog_[i]
    bool getRedoLog(int i, LogRecord &lr) {
        if (i >= redoLog_.size()) {
            return false;
        }
        lr = redoLog_[i];
        return true;
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
            //lr.debugDump();
            cur += lr.getLen();
            i += lr.getLen();
        }
    }

    bool isRecoverNeeded(void) {
        return log_->isRecoverNeeded();
    }
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
  std::vector<LogRecord> redoLog_;
  std::string checkpointNodesInfoFile;
};