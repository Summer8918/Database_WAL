#include "backing_store.hpp"
#include "debug.hpp"
#include <iostream>
#include <ext/stdio_filebuf.h>
#include <unistd.h>
#include <cassert>

/////////////////////////////////////////////////////////////
// Implementation of the one_file_per_object_backing_store //
/////////////////////////////////////////////////////////////
one_file_per_object_backing_store::one_file_per_object_backing_store(std::string rt)
  : root(rt)
{}

//allocate space for a new version of an object
//requires that version be >> any previous version
//logic for this is now handled by the swap space
void one_file_per_object_backing_store::allocate(uint64_t obj_id, uint64_t version) {
  //uint64_t id = nextid++;
  std::string filename = get_filename(obj_id, version);
  std::fstream dummy(filename, std::fstream::out);
  debug(std::cout << "filename:" << filename << std::endl);
  dummy.flush();
  assert(dummy.good());
  //return id;
}

//delete the file associated with an specific version of a node
void one_file_per_object_backing_store::deallocate(uint64_t obj_id, uint64_t version) {
  std::string filename = get_filename(obj_id, version);
  assert(unlink(filename.c_str()) == 0);
}

//return filestream corresponding to an item. Needed for deserialization.
std::iostream * one_file_per_object_backing_store::get(uint64_t obj_id, uint64_t version) {
  __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>;
  
  std::string filename = get_filename(obj_id, version);
  
  fb->open(filename, std::fstream::in | std::fstream::out);
  std::fstream *ios = new std::fstream;
  ios->std::ios::rdbuf(fb);
  ios->exceptions(std::fstream::badbit | std::fstream::failbit | std::fstream::eofbit);
  assert(ios->good());
  
  return ios;
}

//push changes from iostream and close.
void one_file_per_object_backing_store::put(std::iostream *ios)
{
  ios->flush();
  __gnu_cxx::stdio_filebuf<char> *fb = (__gnu_cxx::stdio_filebuf<char> *)ios->rdbuf();
  fsync(fb->fd());
  delete ios;
  delete fb;
}


//Given an object and version, return the filename corresponding to it.
std::string one_file_per_object_backing_store::get_filename(uint64_t obj_id, uint64_t version){

  return root + "/" + std::to_string(obj_id) + "_" + std::to_string(version);

}

std::string one_file_per_object_backing_store::getRootDir(void) {
  return root;
}

LogFileBackingStore::LogFileBackingStore(std::string logFile)
{
    logFile_ = logFile;
    std::ifstream file(logFile_);
    std::string oldLogFile = logFile_ + ".old";
    std::ifstream fileOld(oldLogFile);
    // Check if logFile is open(i.e., it exists)
    if (file.is_open() || fileOld.is_open()) {
        if (file.is_open()) {
            debug(std::cout << "filename:" << logFile << " exists." << std::endl);
            file.close();
        } else {
            debug(std::cout << "filename:" << oldLogFile << " exists." << std::endl);
            fileOld.close();
            assert(std::rename(oldLogFile.c_str(), logFile_.c_str()) == 0);
        }
        logExists_ = true;
    } else {
        debug(std::cout << "new log" << this << std::endl);
        std::fstream dummy(logFile_, std::fstream::out);
        dummy.flush();
        assert(dummy.good());
        dummy.close();
        logExists_ = false;
    }
}

void LogFileBackingStore::appendData(const char* data, int len) {
  std::ofstream file(logFile_, std::ios::app | std::ios::binary);
  assert(file.is_open());
  file.write(data, len);
  file.close();
  // flush file to disk
  file.flush();
}

std::ifstream*  LogFileBackingStore::get(int &len) {
    std::ifstream* filePtr = new std::ifstream(logFile_, std::ios::binary);
    assert(filePtr->is_open()); 

    // Determine the file size
    filePtr->seekg(0, std::ios::end);
    std::streampos fileSize = filePtr->tellg();
    len = (int)fileSize;
    filePtr->seekg(0, std::ios::beg);
    return filePtr;
}

void LogFileBackingStore::put(const char* data, int len) {
    std::string oldLogFile = logFile_ + ".old";
    assert(std::rename(logFile_.c_str(), oldLogFile.c_str()) == 0);
    std::ofstream file(logFile_, std::ofstream::out);
    assert(file.is_open());
    file.write(data, len);
    file.close();
    // flush file to disk
    file.flush();
    // delete old log file
    std::remove(oldLogFile.c_str());
}

// Truncate Log File from the offset begining with len  
void LogFileBackingStore::truncateLogFile(int len) {
    // Open the file in binary mode for both reading and writing
    std::fstream file(logFile_, std::ios::in | std::ios::out | std::ios::binary);
    assert(file.is_open());
    // Determine the file size
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    file.seekg(0, std::ios::beg);

    // Seek to the desired position (offset) in the file
    file.seekg(len, std::ios::beg);

    assert((int)fileSize - len >= 0);
    int newFileLen = (int)fileSize - len;
    // Read data from len to the end of the file
    char buffer[newFileLen];
    char ch;
    int i = 0;
    while (file.get(ch)) {
        buffer[i] = ch;
        i++;
    }
    file.close();
    std::fstream nFile(logFile_, std::ios::in | std::ios::out | std::ios::trunc);
    // Write the data back to the beginning of the file
    for (int j = 0; j < newFileLen; j++) {
        nFile.put(buffer[j]);
    }
    // Close the file
    nFile.close();
    nFile.flush();
}

bool LogFileBackingStore::isRecoverNeeded(void) {
    return logExists_;
}