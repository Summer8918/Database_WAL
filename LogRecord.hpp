
#define DEBUG

#define INVALID_LSN 0xffffffffffffffff
#define INVALID_TRANSACTION_ID 0xffffffffffffffff
#define INVALID_PAGE_ID 0xffffffffffffffff
// Design for txn without previous Lsn
#define NULL_LSN 0xfffffffffffffffe

#define MAX_LOG_RECORD_NUM 64

#define INVALID_KEY 0xffffffffffffffff

const int NODE_ID_LEN = 8;

const int NODE_VERSION_LEN = 8;

enum class LogRecordType {
    INVALID = 0,
    UPDATE_LOG_RECORD,
    DELETE_LOG_RECORD,
    INSERT_LOG_RECORD,
    BEGIN,
    COMMIT,
    CHECKOUT_POINT,
    ABORT,
    SPLIT,
    MERGE,
    /*
    The CLR describes the changes made to undo any actions of a previous update.
    */
    CLR,
};

typedef uint64_t Key;
typedef std::string Value;

struct LogRecordHead {
    int length;
    LogRecordType recType;
    u_int64_t transactionId;
    u_int64_t lsn;
    u_int64_t preLsn;
    u_int64_t pageId;
    int keyLen;
    int beforeValueLen;
    int afterValueLen;
    int nodeIdAndVerLen;
};

const int LOG_RECORD_HEAD_LEN = sizeof(LogRecordHead);
const int KEY_LEN = sizeof(Key);

class LogRecord{
public:
    LogRecord() {}

    // Constructor for ABORT, BEGIN, CLR and COMMIT type
    LogRecord(uint64_t txtId,
                uint64_t lsn, 
                uint64_t prevLsn,
                LogRecordType logRecordType, 
                uint64_t pageId) {
        head_.recType = logRecordType;
        head_.transactionId = txtId;
        head_.lsn = lsn;
        head_.preLsn = prevLsn;
        head_.pageId = pageId;
        head_.afterValueLen = 0;
        head_.beforeValueLen = 0;
        head_.nodeIdAndVerLen = 0;
        head_.keyLen = 0;
        head_.length = LOG_RECORD_HEAD_LEN + head_.afterValueLen 
                    + head_.beforeValueLen + head_.keyLen
                    + head_.nodeIdAndVerLen;
        key_ = INVALID_KEY;
    }

    // Constructor for CHECKPOINT type, pageId is the root object ID and key is the version.
    LogRecord(uint64_t txtId,
                uint64_t lsn,
                uint64_t prevLsn,
                LogRecordType logRecordType,
                uint64_t pageId,
                uint64_t version,
                std::vector<std::pair<uint64_t, uint64_t>> &idAndVers) {
        head_.recType = logRecordType;
        head_.transactionId = txtId;
        head_.lsn = lsn;
        head_.preLsn = prevLsn;
        head_.pageId = pageId;
        head_.afterValueLen = 0;
        head_.beforeValueLen = 0;
        head_.keyLen = sizeof(version);
        head_.nodeIdAndVerLen = idAndVers.size() * (NODE_ID_LEN + NODE_VERSION_LEN);
        head_.length = LOG_RECORD_HEAD_LEN + head_.afterValueLen
                    + head_.beforeValueLen + head_.keyLen
                    + head_.nodeIdAndVerLen;
        key_ = version;
        idAndVers_ = idAndVers;
    }

    // Constructor for DELETE, INSERT and UPDATE without beforeValue
    LogRecord(uint64_t txtId,
                uint64_t lsn, 
                uint64_t prevLsn,
                LogRecordType logRecordType, 
                uint64_t pageId,
                Key k,
                Value val) {
        head_.recType = logRecordType;
        head_.transactionId = txtId;
        head_.lsn = lsn;
        head_.preLsn = prevLsn;
        head_.pageId = pageId;
        head_.keyLen = KEY_LEN;
        head_.nodeIdAndVerLen = 0;
        if (logRecordType == LogRecordType::DELETE_LOG_RECORD) {
            head_.beforeValueLen = val.length();
            head_.afterValueLen = 0;
            beforeValue_ = val;
        } else if (logRecordType == LogRecordType::INSERT_LOG_RECORD || \
                    logRecordType == LogRecordType::UPDATE_LOG_RECORD) {
            head_.afterValueLen = val.length();
            head_.beforeValueLen = 0;
            afterValue_ = val;
        }
        head_.length = LOG_RECORD_HEAD_LEN + head_.afterValueLen 
                    + head_.beforeValueLen + head_.keyLen
                    + head_.nodeIdAndVerLen;
        key_ = k;
    }

    // Constructor for UPDATE type with beforeValue
    LogRecord(uint64_t txtId,
                uint64_t lsn, 
                uint64_t prevLsn,
                LogRecordType logRecordType, 
                uint64_t pageId,
                Key k,
                Value beforeVal,
                Value afterVal) {
        head_.recType = logRecordType;
        head_.transactionId = txtId;
        head_.lsn = lsn;
        head_.preLsn = prevLsn;
        head_.pageId = pageId;
        head_.keyLen = KEY_LEN;
        head_.beforeValueLen = beforeVal.length();
        head_.afterValueLen = afterVal.length();
        head_.nodeIdAndVerLen = 0;
        beforeValue_ = beforeVal;
        afterValue_ = afterVal;

        head_.length = LOG_RECORD_HEAD_LEN + head_.afterValueLen 
                    + head_.beforeValueLen + head_.keyLen +
                    head_.nodeIdAndVerLen;
        key_ = k;
    }

    // Construct LogRecord according to data in binary. Function like deserialize 
    LogRecord(const char *buf, int len) {
        //debug(std::cout << "len: " << len << std::endl);
        assert(len >= LOG_RECORD_HEAD_LEN);
        //debug(std::cout << "LOG_RECORD_HEAD_LEN: " << LOG_RECORD_HEAD_LEN << std::endl);
        memcpy(&head_, buf, LOG_RECORD_HEAD_LEN);
        debug(std::cout << "head_.length: " << head_.length << std::endl);
        assert(len >= head_.length);
        buf += LOG_RECORD_HEAD_LEN;
        memcpy(&key_, buf, head_.keyLen);
        buf += head_.keyLen;
        beforeValue_.assign(buf, head_.beforeValueLen);
        buf += head_.beforeValueLen;
        afterValue_.assign(buf, head_.afterValueLen);
        buf += head_.afterValueLen;
        if (head_.nodeIdAndVerLen > 0) {
            for (int i = 0; i < head_.nodeIdAndVerLen; i += (NODE_VERSION_LEN + NODE_ID_LEN)) {
                std::pair<uint64_t, uint64_t> p;
                memcpy(&p.first, buf, NODE_ID_LEN);
                buf += NODE_ID_LEN;
                memcpy(&p.second, buf, NODE_VERSION_LEN);
                buf += NODE_VERSION_LEN;
                idAndVers_.push_back(p);
            }
        }
    }

    LogRecordHead getHead(void) {
        return this->head_;
    }

    // Dump record for debug purpose
    void debugDump(void) {
        std::ostringstream os;
        os  << "length:" << head_.length
            << " recType" << (int) head_.recType
            << " transactionId:" << head_.transactionId
            << " LSN:" << head_.lsn
            << " preLsn:" << head_.preLsn
            << " pageId:" << head_.pageId
            << " keyLen:" << head_.keyLen
            << " beforeValueLen:" << head_.beforeValueLen
            << " afterValueLen:" << head_.afterValueLen
            << " nodeIdAndVerLen" << head_.nodeIdAndVerLen
            << " Key:" << key_
            << " beforeValue_:" << beforeValue_
            << " afterValue_:" << afterValue_
            << std::endl;
        for (int i = 0; i < idAndVers_.size(); i++) {
            os << "idAndVers_[" << i << "], id:" << idAndVers_[i].first
                << " version:" << idAndVers_[i].second << std::endl;
        }
        debug(std::cout << os.str() << std::endl);
    }

    // The length of LogRecord
    int getLen(void) {
        return head_.length;
    }

    // serialize LogRecord into input buffer, and return the size of the LogRecord.
    int serialize(char *buf, int len) {
        assert(len >= head_.length);
        memcpy(buf, &head_, LOG_RECORD_HEAD_LEN);
        buf += LOG_RECORD_HEAD_LEN;
        if (head_.keyLen > 0) {
            memcpy(buf, &key_, head_.keyLen);
            buf += head_.keyLen;
        }
        if (head_.beforeValueLen > 0) {
            memcpy(buf, beforeValue_.c_str(), head_.beforeValueLen);
            buf += head_.beforeValueLen;
        }
        if (head_.afterValueLen > 0) {
            memcpy(buf, afterValue_.c_str(), head_.afterValueLen);
            buf += head_.afterValueLen;
        }
        if (head_.nodeIdAndVerLen > 0) {
            for (int i = 0; i < idAndVers_.size(); i++) {
                memcpy(buf, &idAndVers_[i].first, NODE_ID_LEN);
                buf += NODE_ID_LEN;
                memcpy(buf, &idAndVers_[i].second, NODE_VERSION_LEN);
                buf += NODE_VERSION_LEN;
            }
        }
        return head_.length;
    }

    u_int64_t getTxtId() {
        return head_.transactionId;
    }

    u_int64_t getLsn() {
        return head_.lsn;
    }

    uint64_t getPreLsn() {
        return head_.preLsn;
    }

    LogRecordType getLogRecType() {
        return head_.recType;
    }

    uint64_t getPageId() {
        return head_.pageId;
    }

    Key getKey() {
        return key_;
    }

    Value getBeforeVal() {
        return beforeValue_;
    }

    Value getAfterVal() {
        return afterValue_;
    }

    LogRecord& operator=(LogRecord &other) {
        if (this == &other) {
            return *this;
        }
        this->head_ = other.getHead();
        this->key_ = other.getKey();
        this->beforeValue_ = other.getBeforeVal();
        this->afterValue_ = other.getAfterVal();
        this->idAndVers_ = other.getIdAndVers();
        return *this;
    }

    std::vector<std::pair<uint64_t, uint64_t>> getIdAndVers(void) {
        return idAndVers_;
    }

    void generateIdAndVersMap(std::unordered_map<uint64_t, uint64_t> &objsMap) {
        for (int i = 0; i < idAndVers_.size(); i++) {
            objsMap[idAndVers_[i].first] = idAndVers_[i].second;
        }
    }
private:
    LogRecordHead head_;
    Key key_;
    Value beforeValue_;
    Value afterValue_;
    std::vector<std::pair<uint64_t, uint64_t>> idAndVers_;
};