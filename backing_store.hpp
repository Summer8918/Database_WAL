// Generic interface to the disk.  Used by swap_space to store
// objects.

#ifndef BACKING_STORE_HPP
#define BACKING_STORE_HPP

#include <cstdint>
#include <cstddef>
#include <iostream>
#include <fstream>

class backing_store {
public:
  virtual void   allocate(uint64_t obj_id, uint64_t version) = 0;
  virtual void deallocate(uint64_t obj_id, uint64_t version) = 0;
  virtual std::iostream * get(uint64_t obj_id, uint64_t version) = 0;
  virtual void            put(std::iostream *ios) = 0;
  virtual std::string getRootDir(void) = 0;
};

class one_file_per_object_backing_store: public backing_store {
public:
  one_file_per_object_backing_store(std::string rt);
  void	  allocate(uint64_t obj_id, uint64_t version);
  void		  deallocate(uint64_t obj_id, uint64_t version);
  std::iostream * get(uint64_t obj_id, uint64_t version);
  void            put(std::iostream *ios);
  std::string get_filename(uint64_t obj_id, uint64_t version);
  std::string getRootDir(void);
private:
  std::string	root;
};

class LogFileBackingStore {
public:
    LogFileBackingStore(std::string);
    void appendData(const char* data, int len);
    std::ifstream * get(int &len);
    void put(const char* data, int len);
    void truncateLogFile(int len);
    bool isRecoverNeeded(void);
    void unableRecover(void);
private:
    std::string logFile_;
    bool logExists_;
};

#endif // BACKING_STORE_HPP