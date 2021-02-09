//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/env.h"
#include "env/env_octopus.h"


#ifndef ROCKSDB_OCTOPUS_FILE_C
#define ROCKSDB_OCTOPUS_FILE_C

#include <stdio.h>
#include <time.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include "logging/logging.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

#define OCTOPUS_EXISTS 0
#define OCTOPUS_DOESNT_EXIST -1
#define OCTOPUS_SUCCESS 0

//
// This file defines an OCTOPUS environment for rocksdb. It uses the libhdfs
// api to access OCTOPUS. All OCTOPUS files created by one instance of rocksdb
// will reside on the same OCTOPUS cluster.
//

namespace ROCKSDB_NAMESPACE {

namespace {

// Log error message
static Status IOError(const std::string& context, int err_number) {
  return (err_number == ENOSPC)
             ? Status::NoSpace(context, strerror(err_number))
             : (err_number == ENOENT)
                   ? Status::PathNotFound(context, strerror(err_number))
                   : Status::IOError(context, strerror(err_number));
}

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
static Logger* mylog = nullptr;

// Used for reading a file from OCTOPUS. It implements both sequential-read
// access methods as well as random read access methods.
class OctopusReadableFile : virtual public SequentialFile,
                         virtual public RandomAccessFile {
 private:
  hdfsFS fileSys_;
  std::string filename_;
  hdfsFile hfile_;

 public:
  OctopusReadableFile(hdfsFS fileSys, const std::string& fname)
      : fileSys_(fileSys), filename_(fname), hfile_(nullptr) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile opening file %s\n",
                    filename_.c_str());
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_RDONLY, 0, 0, 0);
    ROCKS_LOG_DEBUG(mylog,
                    "[hdfs] OctopusReadableFile opened file %s hfile_=0x%p\n",
                    filename_.c_str(), hfile_);
  }

  virtual ~OctopusReadableFile() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile closing file %s\n",
                    filename_.c_str());
    hdfsCloseFile(fileSys_, hfile_);
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile closed file %s\n",
                    filename_.c_str());
    hfile_ = nullptr;
  }

  bool isValid() {
    return hfile_ != nullptr;
  }

  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile reading %s %ld\n",
                    filename_.c_str(), n);

    char* buffer = scratch;
    size_t total_bytes_read = 0;
    tSize bytes_read = 0;
    tSize remaining_bytes = (tSize)n;

    // Read a total of n bytes repeatedly until we hit error or eof
    while (remaining_bytes > 0) {
      bytes_read = hdfsRead(fileSys_, hfile_, buffer, remaining_bytes);
      if (bytes_read <= 0) {
        break;
      }
      assert(bytes_read <= remaining_bytes);

      total_bytes_read += bytes_read;
      remaining_bytes -= bytes_read;
      buffer += bytes_read;
    }
    assert(total_bytes_read <= n);

    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile read %s\n",
                    filename_.c_str());

    if (bytes_read < 0) {
      s = IOError(filename_, errno);
    } else {
      *result = Slice(scratch, total_bytes_read);
    }

    return s;
  }

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile preading %s\n",
                    filename_.c_str());
    tSize bytes_read =
        hdfsPread(fileSys_, hfile_, offset, static_cast<void*>(scratch),
                  static_cast<tSize>(n));
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile pread %s\n",
                    filename_.c_str());
    *result = Slice(scratch, (bytes_read < 0) ? 0 : bytes_read);
    if (bytes_read < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile skip %s\n",
                    filename_.c_str());
    // get current offset from file
    tOffset current = hdfsTell(fileSys_, hfile_);
    if (current < 0) {
      return IOError(filename_, errno);
    }
    // seek to new offset in file
    tOffset newoffset = current + n;
    int val = hdfsSeek(fileSys_, hfile_, newoffset);
    if (val < 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

 private:

  // returns true if we are at the end of file, false otherwise
  bool feof() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile feof %s\n",
                    filename_.c_str());
    if (hdfsTell(fileSys_, hfile_) == fileSize()) {
      return true;
    }
    return false;
  }

  // the current size of the file
  tOffset fileSize() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusReadableFile fileSize %s\n",
                    filename_.c_str());
    hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, filename_.c_str());
    tOffset size = 0L;
    if (pFileInfo != nullptr) {
      size = pFileInfo->mSize;
      hdfsFreeFileInfo(pFileInfo, 1);
    } else {
      throw OctopusFatalException("fileSize on unknown file " + filename_);
    }
    return size;
  }
};

// Appends to an existing file in OCTOPUS.
class OctopusWritableFile: public WritableFile {
 private:
  hdfsFS fileSys_;
  std::string filename_;
  hdfsFile hfile_;

 public:
  OctopusWritableFile(hdfsFS fileSys, const std::string& fname,
                   const EnvOptions& options)
      : WritableFile(options),
        fileSys_(fileSys),
        filename_(fname),
        hfile_(nullptr) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile opening %s\n",
                    filename_.c_str());
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_WRONLY, 0, 0, 0);
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile opened %s\n",
                    filename_.c_str());
    assert(hfile_ != nullptr);
  }
  virtual ~OctopusWritableFile() {
    if (hfile_ != nullptr) {
      ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile closing %s\n",
                      filename_.c_str());
      hdfsCloseFile(fileSys_, hfile_);
      ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile closed %s\n",
                      filename_.c_str());
      hfile_ = nullptr;
    }
  }

  // If the file was successfully created, then this returns true.
  // Otherwise returns false.
  bool isValid() {
    return hfile_ != nullptr;
  }

  // The name of the file, mostly needed for debug logging.
  const std::string& getName() {
    return filename_;
  }

  virtual Status Append(const Slice& data) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile Append %s\n",
                    filename_.c_str());
    const char* src = data.data();
    size_t left = data.size();
    size_t ret = hdfsWrite(fileSys_, hfile_, src, static_cast<tSize>(left));
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile Appended %s\n",
                    filename_.c_str());
    if (ret != left) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile Sync %s\n",
                    filename_.c_str());
    if (hdfsFlush(fileSys_, hfile_) == -1) {
      return IOError(filename_, errno);
    }
    if (hdfsHSync(fileSys_, hfile_) == -1) {
      return IOError(filename_, errno);
    }
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile Synced %s\n",
                    filename_.c_str());
    return Status::OK();
  }

  // This is used by OctopusLogger to write data to the debug log file
  virtual Status Append(const char* src, size_t size) {
    if (hdfsWrite(fileSys_, hfile_, src, static_cast<tSize>(size)) !=
        static_cast<tSize>(size)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile closing %s\n",
                    filename_.c_str());
    if (hdfsCloseFile(fileSys_, hfile_) != 0) {
      return IOError(filename_, errno);
    }
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusWritableFile closed %s\n",
                    filename_.c_str());
    hfile_ = nullptr;
    return Status::OK();
  }
};

// The object that implements the debug logs to reside in OCTOPUS.
class OctopusLogger : public Logger {
 private:
  OctopusWritableFile* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread

  Status OctopusCloseHelper() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusLogger closed %s\n",
                    file_->getName().c_str());
    if (mylog != nullptr && mylog == this) {
      mylog = nullptr;
    }
    return Status::OK();
  }

 protected:
  virtual Status CloseImpl() override { return OctopusCloseHelper(); }

 public:
  OctopusLogger(OctopusWritableFile* f, uint64_t (*gettid)())
      : file_(f), gettid_(gettid) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] OctopusLogger opened %s\n",
                    file_->getName().c_str());
  }

  ~OctopusLogger() override {
    if (!closed_) {
      closed_ = true;
      OctopusCloseHelper();
    }
  }

  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      file_->Append(base, p-base);
      file_->Flush();
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

}  // namespace

// Finally, the hdfs environment

const std::string OctopusEnv::kProto = "hdfs://";
const std::string OctopusEnv::pathsep = "/";

// open a file for sequential reading
Status OctopusEnv::NewSequentialFile(const std::string& fname,
                                  std::unique_ptr<SequentialFile>* result,
                                  const EnvOptions& /*options*/) {
  result->reset();
  OctopusReadableFile* f = new OctopusReadableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<SequentialFile*>(f));
  return Status::OK();
}

// open a file for random reading
Status OctopusEnv::NewRandomAccessFile(const std::string& fname,
                                    std::unique_ptr<RandomAccessFile>* result,
                                    const EnvOptions& /*options*/) {
  result->reset();
  OctopusReadableFile* f = new OctopusReadableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<RandomAccessFile*>(f));
  return Status::OK();
}

// create a new file for writing
Status OctopusEnv::NewWritableFile(const std::string& fname,
                                std::unique_ptr<WritableFile>* result,
                                const EnvOptions& options) {
  result->reset();
  Status s;
  OctopusWritableFile* f = new OctopusWritableFile(fileSys_, fname, options);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<WritableFile*>(f));
  return Status::OK();
}

class OctopusDirectory : public Directory {
 public:
  explicit OctopusDirectory(int fd) : fd_(fd) {}
  ~OctopusDirectory() {}

  Status Fsync() override { return Status::OK(); }

  int GetFd() const { return fd_; }

 private:
  int fd_;
};

Status OctopusEnv::NewDirectory(const std::string& name,
                             std::unique_ptr<Directory>* result) {
  int value = hdfsExists(fileSys_, name.c_str());
  switch (value) {
    case OCTOPUS_EXISTS:
      result->reset(new OctopusDirectory(0));
      return Status::OK();
    default:  // fail if the directory doesn't exist
      ROCKS_LOG_FATAL(mylog, "NewDirectory hdfsExists call failed");
      throw OctopusFatalException("hdfsExists call failed with error " +
                               ToString(value) + " on path " + name +
                               ".\n");
  }
}

Status OctopusEnv::FileExists(const std::string& fname) {
  int value = hdfsExists(fileSys_, fname.c_str());
  switch (value) {
    case OCTOPUS_EXISTS:
      return Status::OK();
    case OCTOPUS_DOESNT_EXIST:
      return Status::NotFound();
    default:  // anything else should be an error
      ROCKS_LOG_FATAL(mylog, "FileExists hdfsExists call failed");
      return Status::IOError("hdfsExists call failed with error " +
                             ToString(value) + " on path " + fname + ".\n");
  }
}

Status OctopusEnv::GetChildren(const std::string& path,
                            std::vector<std::string>* result) {
  int value = hdfsExists(fileSys_, path.c_str());
  switch (value) {
    case OCTOPUS_EXISTS: {  // directory exists
    int numEntries = 0;
    hdfsFileInfo* pOctopusFileInfo = 0;
    pOctopusFileInfo = hdfsListDirectory(fileSys_, path.c_str(), &numEntries);
    if (numEntries >= 0) {
      for(int i = 0; i < numEntries; i++) {
        std::string pathname(pOctopusFileInfo[i].mName);
        size_t pos = pathname.rfind("/");
        if (std::string::npos != pos) {
          result->push_back(pathname.substr(pos + 1));
        }
      }
      if (pOctopusFileInfo != nullptr) {
        hdfsFreeFileInfo(pOctopusFileInfo, numEntries);
      }
    } else {
      // numEntries < 0 indicates error
      ROCKS_LOG_FATAL(mylog, "hdfsListDirectory call failed with error ");
      throw OctopusFatalException(
          "hdfsListDirectory call failed negative error.\n");
    }
    break;
  }
  case OCTOPUS_DOESNT_EXIST:  // directory does not exist, exit
    return Status::NotFound();
  default:          // anything else should be an error
    ROCKS_LOG_FATAL(mylog, "GetChildren hdfsExists call failed");
    throw OctopusFatalException("hdfsExists call failed with error " +
                             ToString(value) + ".\n");
  }
  return Status::OK();
}

Status OctopusEnv::DeleteFile(const std::string& fname) {
  if (hdfsDelete(fileSys_, fname.c_str(), 1) == 0) {
    return Status::OK();
  }
  return IOError(fname, errno);
};

Status OctopusEnv::CreateDir(const std::string& name) {
  if (hdfsCreateDirectory(fileSys_, name.c_str()) == 0) {
    return Status::OK();
  }
  return IOError(name, errno);
};

Status OctopusEnv::CreateDirIfMissing(const std::string& name) {
  const int value = hdfsExists(fileSys_, name.c_str());
  //  Not atomic. state might change b/w hdfsExists and CreateDir.
  switch (value) {
    case OCTOPUS_EXISTS:
    return Status::OK();
    case OCTOPUS_DOESNT_EXIST:
    return CreateDir(name);
    default:  // anything else should be an error
      ROCKS_LOG_FATAL(mylog, "CreateDirIfMissing hdfsExists call failed");
      throw OctopusFatalException("hdfsExists call failed with error " +
                               ToString(value) + ".\n");
  }
};

Status OctopusEnv::DeleteDir(const std::string& name) {
  return DeleteFile(name);
};

Status OctopusEnv::GetFileSize(const std::string& fname, uint64_t* size) {
  *size = 0L;
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, fname.c_str());
  if (pFileInfo != nullptr) {
    *size = pFileInfo->mSize;
    hdfsFreeFileInfo(pFileInfo, 1);
    return Status::OK();
  }
  return IOError(fname, errno);
}

Status OctopusEnv::GetFileModificationTime(const std::string& fname,
                                        uint64_t* time) {
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, fname.c_str());
  if (pFileInfo != nullptr) {
    *time = static_cast<uint64_t>(pFileInfo->mLastMod);
    hdfsFreeFileInfo(pFileInfo, 1);
    return Status::OK();
  }
  return IOError(fname, errno);

}

// The rename is not atomic. OCTOPUS does not allow a renaming if the
// target already exists. So, we delete the target before attempting the
// rename.
Status OctopusEnv::RenameFile(const std::string& src, const std::string& target) {
  hdfsDelete(fileSys_, target.c_str(), 1);
  if (hdfsRename(fileSys_, src.c_str(), target.c_str()) == 0) {
    return Status::OK();
  }
  return IOError(src, errno);
}

Status OctopusEnv::LockFile(const std::string& /*fname*/, FileLock** lock) {
  // there isn's a very good way to atomically check and create
  // a file via libhdfs
  *lock = nullptr;
  return Status::OK();
}

Status OctopusEnv::UnlockFile(FileLock* /*lock*/) { return Status::OK(); }

Status OctopusEnv::NewLogger(const std::string& fname,
                          std::shared_ptr<Logger>* result) {
  // EnvOptions is used exclusively for its `strict_bytes_per_sync` value. That
  // option is only intended for WAL/flush/compaction writes, so turn it off in
  // the logger.
  EnvOptions options;
  options.strict_bytes_per_sync = false;
  OctopusWritableFile* f = new OctopusWritableFile(fileSys_, fname, options);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  OctopusLogger* h = new OctopusLogger(f, &OctopusEnv::gettid);
  result->reset(h);
  if (mylog == nullptr) {
    // mylog = h; // uncomment this for detailed logging
  }
  return Status::OK();
}

Status OctopusEnv::IsDirectory(const std::string& path, bool* is_dir) {
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, path.c_str());
  if (pFileInfo != nullptr) {
    if (is_dir != nullptr) {
      *is_dir = (pFileInfo->mKind == kObjectKindDirectory);
    }
    hdfsFreeFileInfo(pFileInfo, 1);
    return Status::OK();
  }
  return IOError(path, errno);
}

// The factory method for creating an OCTOPUS Env
Status NewOctopusEnv(Env** hdfs_env, const std::string& fsname) {
  *hdfs_env = new OctopusEnv(fsname);
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE

#endif // ROCKSDB_OCTOPUS_FILE_C

#else // USE_OCTOPUS

// dummy placeholders used when OCTOPUS is not available
namespace ROCKSDB_NAMESPACE {
Status OctopusEnv::NewSequentialFile(const std::string& /*fname*/,
                                  std::unique_ptr<SequentialFile>* /*result*/,
                                  const EnvOptions& /*options*/) {
  return Status::NotSupported("Not compiled with hdfs support");
}

 Status NewOctopusEnv(Env** /*hdfs_env*/, const std::string& /*fsname*/) {
   return Status::NotSupported("Not compiled with hdfs support");
 }
 }  // namespace ROCKSDB_NAMESPACE

