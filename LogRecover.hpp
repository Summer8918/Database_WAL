#ifndef LOG_RECOVER_HPP
#define LOG_RECOVER_HPP

#include "LogManager.hpp"
#include "betree.hpp"
#include <fstream>

class LogRecover {
private:
    LogManager& logManager_;
    betree<uint64_t, std::string>& betree_;
    std::string logDir_;

public:
    LogRecover(LogManager& logManager, betree<uint64_t, std::string>& betree, const std::string& logDir)
        : logManager_(logManager), betree_(betree), logDir_(logDir) {}

    void recover() {
        std::ifstream logFile("log.txt", std::ios::binary);  // Assuming you've opened the log file like this

        std::ofstream resFile("res.txt");  // Open "res.txt" for writing

        if (!logFile.is_open()) {
            // No log file to recover from
            
            return;
        }

        LogRecord lastCheckpoint;
        LogRecord lastRoot;

        // Read the log file to find the last checkpoint and last root
        while (logFile) {
            char buffer[LOG_RECORD_HEAD_LEN]; // Assuming this is the maximum size of a log record
            logFile.read(buffer, sizeof(buffer));

            if (logFile.gcount() == 0) {
                break; // End of file
            }

            LogRecord record(buffer, logFile.gcount());

            if (record.getLogRecType() == LogRecordType::CHECKOUT_POINT) {
                lastCheckpoint = record;
            }
            // Assuming there's a ROOT_LOG_RECORD type to represent the root
            // else if (record.getLogRecType() == LogRecordType::ROOT_LOG_RECORD) {
            //     lastRoot = record;
            // }
        }

        // Start recovery from the last checkpoint
        logFile.clear(); // Clear EOF flag
        logFile.seekg(lastCheckpoint.getLsn()); // Seek to the position of the last checkpoint

        while (logFile) {
            char buffer[LOG_RECORD_HEAD_LEN];
            logFile.read(buffer, sizeof(buffer));

            if (logFile.gcount() == 0) {
                break; // End of file
            }

            LogRecord record(buffer, logFile.gcount());

            // Replay the log record
            switch (record.getLogRecType()) {
                case LogRecordType::INSERT_LOG_RECORD:
                    betree_.insert(record.getKey(), record.getAfterVal());
                    resFile << "INSERT: Key=" << record.getKey() << ", Value=" << record.getAfterVal() << std::endl;
                    
                    break;
                case LogRecordType::UPDATE_LOG_RECORD:
                    betree_.update(record.getKey(), record.getAfterVal());
                    resFile << "UPDATE: Key=" << record.getKey() << ", Value=" << record.getAfterVal() << std::endl;
                    break;
                case LogRecordType::DELETE_LOG_RECORD:
                    betree_.erase(record.getKey());
                    resFile << "DELETE: Key=" << record.getKey() << std::endl;
                    break;
                // Handle other log record types as needed
            }
            
        }

        logFile.close();
        resFile.close();
    }
};

#endif // LOG_RECOVER_HPP
