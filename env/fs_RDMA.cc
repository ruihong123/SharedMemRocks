//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#if !defined(OS_WIN)

#include <dirent.h>
#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
#include <dlfcn.h>
#endif
#include <errno.h>
#include <fcntl.h>

#if defined(OS_LINUX)
#include <linux/fs.h>
#endif
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#if defined(OS_LINUX) || defined(OS_SOLARIS) || defined(OS_ANDROID)
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#endif
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <algorithm>

// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD)
#elif defined(__MACH__)
#include <Availability.h>
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif
#include <deque>
#include <set>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "env/io_posix.h"
#include "logging/logging.h"
#include "logging/posix_logger.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "port/port.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression_context_cache.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"
#include "include/rocksdb/rdma.h"

#if !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif
#if !defined(XFS_SUPER_MAGIC)
#define XFS_SUPER_MAGIC 0x58465342
#endif
#if !defined(EXT4_SUPER_MAGIC)
#define EXT4_SUPER_MAGIC 0xEF53
#endif

namespace ROCKSDB_NAMESPACE {

namespace {

inline mode_t GetDBFileMode(bool allow_non_owner_access) {
  return allow_non_owner_access ? 0644 : 0600;
}

static uint64_t gettid() {
  return Env::Default()->GetThreadID();
}

// list of pathnames that are locked
// Only used for error message.
struct LockHoldingInfo {
  int64_t acquire_time;
  uint64_t acquiring_thread;
};

static std::map<std::string, LockHoldingInfo> locked_files;
static port::Mutex mutex_locked_files;

//static int LockOrUnlock(int fd, bool lock) {
//  errno = 0;
//  struct flock f;
//  memset(&f, 0, sizeof(f));
//  f.l_type = (lock ? F_WRLCK : F_UNLCK);
//  f.l_whence = SEEK_SET;
//  f.l_start = 0;
//  f.l_len = 0;  // Lock/unlock entire file
//  int value = fcntl(fd, F_SETLK, &f);
//
//  return value;
//}

//class PosixFileLock : public FileLock {
// public:
//  int fd_;
//  std::string filename;
//};

int cloexec_flags(int flags, const EnvOptions* options) {
  // If the system supports opening the file with cloexec enabled,
  // do so, as this avoids a race condition if a db is opened around
  // the same time that a child process is forked
#ifdef O_CLOEXEC
  if (options == nullptr || options->set_fd_cloexec) {
    flags |= O_CLOEXEC;
  }
#else
  (void)options;
#endif
  return flags;
}
//Add rdma manager into filesystem, maitaining a file to RDMA Placeholder table.
class RDMAFileSystem : public FileSystem {
 public:
  RDMAFileSystem();

  const char* Name() const override { return "Posix File System"; }

  ~RDMAFileSystem();
  enum Open_Type {readtype, write_reopen, write_new, rwtype};
  void SetFD_CLOEXEC(int fd, const EnvOptions* options) {
    if ((options == nullptr || options->set_fd_cloexec) && fd > 0) {
      fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
    }
  }
  bool RDMA_Rename(const std::string &new_name, const std::string &old_name){
    auto entry = file_to_sst_meta.find(old_name);
    if (entry == file_to_sst_meta.end()) {
      std::cout << "File rename did not find the old name" <<std::endl;
      return false;
    } else {
        auto const value = std::move(entry->second);
        file_to_sst_meta.erase(entry);
        file_to_sst_meta.insert({new_name, std::move(value)});
        return true;
    }

  }
  // Delete a file in rdma file system, return 0 mean success, return 1 mean
  //did not find the key in the map, 2 means find keys in map larger than 1.
  int RDMA_Delete_File(const std::string& fname){
    //First find out the meta_data pointer, then search in the map weather there are
    // other file name link to the same file. If it is the last one, unpin the region in the
    // remote buffer pool.
    SST_Metadata* file_meta = file_to_sst_meta.at(fname);
    int erasenum = file_to_sst_meta.erase(fname);// delete this file name
    if (erasenum==0) return 1; // the file name should only have one entry
    else if (erasenum>1) return 2;
    auto ptr = file_to_sst_meta.begin();
    while(ptr != file_to_sst_meta.end())
    // check whether there is other filename link to the same file
    {
      // Check if value of this entry matches with given value
      if(ptr->second == file_meta)
        return 0;// if find then return.
      // Go to next entry in map
      ptr++;
    }
    //if not find other filename, then make the memory slot not in use (delete the file)
    int buff_offset = static_cast<char*>(file_meta->mr->addr) - static_cast<char*>(file_meta->map_pointer->addr);
    assert(buff_offset%kDefaultPageSize == 0);
    if (rdma_mg_->Remote_Mem_Bitmap->at(file_meta->map_pointer).deallocate_memory_slot(buff_offset)) {
      // delete remove the flage sucessfully
      delete file_meta->mr;
      delete file_meta;
      return 0;
    }
    else{
      std::cout << "clear the flag in in_use_table failed" << std::endl;
      return 3;
    }



  }

  IOStatus RDMA_open(const std::string& file_name, SST_Metadata*& sst_meta, Open_Type type){
    //For write_reopen and read type if not found in the mapping table, then
    //it is an error.
    //for write_new type, if there is one file existed with the same name, then overwrite it.otherwise
    //create a new one
    //for read&write type, try to find in the map table first, if missing then create a
    //new one
    if(type == write_new){
      if (file_to_sst_meta.find(file_name) == file_to_sst_meta.end()) {
        // std container always copy the value to the container, Don't worry.
        rdma_mg_->Allocate_Remote_RDMA_Slot(file_name, sst_meta);
        file_to_sst_meta[file_name] = sst_meta;
        return IOStatus::OK();
      } else {
        file_to_sst_meta[file_name]->file_size = 0;// truncate the existing file (need concurrency control)
        sst_meta = file_to_sst_meta[file_name];
        return IOStatus::OK();      }
    }
    if(type == rwtype) {
      if (file_to_sst_meta.find(file_name) == file_to_sst_meta.end()) {
        // std container always copy the value to the container, Don't worry.
        rdma_mg_->Allocate_Remote_RDMA_Slot(file_name, sst_meta);
        file_to_sst_meta[file_name] = sst_meta;
        return IOStatus::OK();
      } else {
        sst_meta = file_to_sst_meta[file_name];
        return IOStatus::OK();
      }
    }
    if(type == write_reopen || type == readtype){
      if (file_to_sst_meta.find(file_name) == file_to_sst_meta.end()) {
        // std container always copy the value to the container, Don't worry.
        errno = ENOENT;
        return IOError("While open a file for random read", file_name, errno);
      } else {
        sst_meta = file_to_sst_meta[file_name];
        return IOStatus::OK();
      }
    }
    return IOStatus::OK();
  }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* /*dbg*/) override {
    result->reset();
    int fd = -1;
    int flags = cloexec_flags(O_RDONLY, &options);
    FILE* file = nullptr;

    if (options.use_direct_reads && !options.use_mmap_reads) {
#ifdef ROCKSDB_LITE
      return IOStatus::IOError(fname,
                               "Direct I/O not supported in RocksDB lite");
#endif  // !ROCKSDB_LITE
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_SOLARIS)
      flags |= O_DIRECT;
      TEST_SYNC_POINT_CALLBACK("NewSequentialFile:O_DIRECT", &flags);
#endif
    }

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, GetDBFileMode(allow_non_owner_access_));
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      return IOError("While opening a file for sequentially reading", fname,
                     errno);
    }

    SetFD_CLOEXEC(fd, &options);

    if (options.use_direct_reads && !options.use_mmap_reads) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        return IOError("While fcntl NoCache", fname, errno);
      }
#endif
    } else {
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        file = fdopen(fd, "r");
      } while (file == nullptr && errno == EINTR);
      if (file == nullptr) {
        close(fd);
        return IOError("While opening file for sequentially read", fname,
                       errno);
      }
    }
    result->reset(new PosixSequentialFile(
        fname, file, fd, GetLogicalBlockSizeForReadIfNeeded(options, fname, fd),
        options, rdma_mg_));
    return IOStatus::OK();
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* /*dbg*/) override {
    IOStatus s = IOStatus::OK();
    result->reset();
    SST_Metadata* meta_data = nullptr;
    RDMA_open(fname, meta_data, readtype);
    if (!options.use_mmap_reads) { //Notice: check here when debugging.
      result->reset(new PosixRandomAccessFile(meta_data, kDefaultPageSize,
                                              options, rdma_mg_));
    }
    else{
      std::cout << "Please turn off MMAP option"<< std::endl;
    }
    return s;
  }

  virtual IOStatus OpenWritableFile(const std::string& fname,
                                    const FileOptions& options,
                                    bool reopen,
                                    std::unique_ptr<FSWritableFile>* result,
                                    IODebugContext* /*dbg*/) {

    result->reset();
    IOStatus s;
    Open_Type type = (reopen) ? (write_reopen) : (write_new);
    SST_Metadata* meta_data = nullptr;
    RDMA_open(fname, meta_data, type);
    if (!options.use_mmap_reads){
      result->reset(new PosixWritableFile(meta_data, kDefaultPageSize, options,
                                          rdma_mg_));
    }else{
      std::cout << "Please turn off Mmap option" << std::endl;
    }
    return s;
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    return OpenWritableFile(fname, options, false, result, dbg);
  }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    return OpenWritableFile(fname, options, true, result, dbg);
  }
  //Find a file rename it and open it as writable file.
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* /*dbg*/) override {
    IOStatus s;
    bool success = RDMA_Rename(fname, old_fname);
    if (success){
      result->reset();

      Open_Type type = write_reopen;
      SST_Metadata* meta_data = nullptr;
      RDMA_open(fname, meta_data, type);
      if (!options.use_mmap_reads){
        result->reset(new PosixWritableFile(meta_data, kDefaultPageSize, options,
                                            rdma_mg_));
      }else{
        std::cout << "Please turn off Mmap option" << std::endl;

      }
    }else{
      IOError("Renaming the file failed", old_fname, errno);
    }



    return s;
  }

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* /*dbg*/) override {
    int fd = -1;
    int flags = cloexec_flags(O_RDWR, &options);

    while (fd < 0) {
      IOSTATS_TIMER_GUARD(open_nanos);

      fd = open(fname.c_str(), flags, GetDBFileMode(allow_non_owner_access_));
      if (fd < 0) {
        // Error while opening the file
        if (errno == EINTR) {
          continue;
        }
        return IOError("While open file for random read/write", fname, errno);
      }
    }

    SetFD_CLOEXEC(fd, &options);
    result->reset(new PosixRandomRWFile(fname, fd, options, rdma_mg_));
    return IOStatus::OK();
  }

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    int fd = -1;
    IOStatus status;
    int flags = cloexec_flags(O_RDWR, nullptr);

    while (fd < 0) {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, 0644);
      if (fd < 0) {
        // Error while opening the file
        if (errno == EINTR) {
          continue;
        }
        status =
            IOError("While open file for raw mmap buffer access", fname, errno);
        break;
      }
    }
    uint64_t size;
    if (status.ok()) {
      IOOptions opts;
      status = GetFileSize(fname, opts, &size, nullptr);
    }
    void* base = nullptr;
    if (status.ok()) {
      base = mmap(nullptr, static_cast<size_t>(size), PROT_READ | PROT_WRITE,
                  MAP_SHARED, fd, 0);
      if (base == MAP_FAILED) {
        status = IOError("while mmap file for read", fname, errno);
      }
    }
    if (status.ok()) {
      result->reset(
          new PosixMemoryMappedFileBuffer(base, static_cast<size_t>(size)));
    }
    if (fd >= 0) {
      // don't need to keep it open after mmap has been called
      close(fd);
    }
    return status;
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& /*opts*/,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* /*dbg*/) override {
    result->reset();
    int fd;
    int flags = cloexec_flags(0, nullptr);
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(name.c_str(), flags);
    }
    if (fd < 0) {
      return IOError("While open directory", name, errno);
    } else {
      result->reset(new PosixDirectory(fd));
    }
    return IOStatus::OK();
  }

  IOStatus NewLogger(const std::string& fname, const IOOptions& /*opts*/,
                   std::shared_ptr<Logger>* result,
                   IODebugContext* /*dbg*/) override {
    FILE* f;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      f = fopen(fname.c_str(),
                "w"
#ifdef __GLIBC_PREREQ
#if __GLIBC_PREREQ(2, 7)
                "e"  // glibc extension to enable O_CLOEXEC
#endif
#endif
      );
    }
    if (f == nullptr) {
      result->reset();
      return status_to_io_status(
              IOError("when fopen a file for new logger", fname, errno));
    } else {
      int fd = fileno(f);
#ifdef ROCKSDB_FALLOCATE_PRESENT
      fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, 4 * 1024);
#endif
      SetFD_CLOEXEC(fd, nullptr);
      result->reset(new PosixLogger(f, &gettid, Env::Default()));
      return IOStatus::OK();
    }
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    int result = access(fname.c_str(), F_OK);

    if (result == 0) {
      return IOStatus::OK();
    }

    int err = errno;
    switch (err) {
      case EACCES:
      case ELOOP:
      case ENAMETOOLONG:
      case ENOENT:
      case ENOTDIR:
        return IOStatus::NotFound();
      default:
        assert(err == EIO || err == ENOMEM);
        return IOStatus::IOError("Unexpected error(" + ToString(err) +
                                 ") accessing file `" + fname + "' ");
    }
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& /*opts*/,
                       std::vector<std::string>* result,
                       IODebugContext* /*dbg*/) override {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == nullptr) {
      switch (errno) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
          return IOStatus::NotFound();
        default:
          return IOError("While opendir", dir, errno);
      }
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return IOStatus::OK();
  }
// First try to delete file in the disk file system, if not found then delete in
  //RDMA file system.
  IOStatus DeleteFile(const std::string& fname, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    IOStatus result = IOStatus::OK();
    if (unlink(fname.c_str()) != 0) {
      result = IOError("while unlink() file", fname, errno);
    }
    if (result.ok()) return result;
    else{
      // Otherwise it is a RDMA file, delete it through the RDMA file delete.
      if (RDMA_Delete_File(fname)!=0){
        result = IOError("while RDMA unlink() file, error occur", fname, errno);
      }
      else result = IOStatus::OK();
    }

    return result;
  }

  IOStatus CreateDir(const std::string& name, const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    if (mkdir(name.c_str(), 0755) != 0) {
      return IOError("While mkdir", name, errno);
    }
    return IOStatus::OK();
  }

  IOStatus CreateDirIfMissing(const std::string& name,
                              const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) override {
    if (mkdir(name.c_str(), 0755) != 0) {
      if (errno != EEXIST) {
        return IOError("While mkdir if missing", name, errno);
      } else if (!DirExists(name)) {  // Check that name is actually a
                                      // directory.
        // Message is taken from mkdir
        return IOStatus::IOError("`" + name +
                                 "' exists but is not a directory");
      }
    }
    return IOStatus::OK();
  }

  IOStatus DeleteDir(const std::string& name, const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    if (rmdir(name.c_str()) != 0) {
      return IOError("file rmdir", name, errno);
    }
    return IOStatus::OK();
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& /*opts*/,
                       uint64_t* size, IODebugContext* /*dbg*/) override {
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      return IOError("while stat a file for size", fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return IOStatus::OK();
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*opts*/,
                                   uint64_t* file_mtime,
                                   IODebugContext* /*dbg*/) override {
    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
      return IOError("while stat a file for modification time", fname, errno);
    }
    *file_mtime = static_cast<uint64_t>(s.st_mtime);
    return IOStatus::OK();
  }

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    if (rename(src.c_str(), target.c_str()) != 0 || !RDMA_Rename(target.c_str(), src.c_str())) {

      return IOError("While renaming a file to " + target, src, errno);
    }
    return IOStatus::OK();

  }

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    if (link(src.c_str(), target.c_str()) != 0) {
      if (errno == EXDEV) {
        return IOStatus::NotSupported("No cross FS links allowed");
      }
      return IOError("while link file to " + target, src, errno);
    }
    return IOStatus::OK();
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& /*opts*/,
                        uint64_t* count, IODebugContext* /*dbg*/) override {
    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
      return IOError("while stat a file for num file links", fname, errno);
    }
    *count = static_cast<uint64_t>(s.st_nlink);
    return IOStatus::OK();
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& /*opts*/, bool* res,
                        IODebugContext* /*dbg*/) override {
    struct stat statbuf[2];
    if (stat(first.c_str(), &statbuf[0]) != 0) {
      return IOError("stat file", first, errno);
    }
    if (stat(second.c_str(), &statbuf[1]) != 0) {
      return IOError("stat file", second, errno);
    }

    if (major(statbuf[0].st_dev) != major(statbuf[1].st_dev) ||
        minor(statbuf[0].st_dev) != minor(statbuf[1].st_dev) ||
        statbuf[0].st_ino != statbuf[1].st_ino) {
      *res = false;
    } else {
      *res = true;
    }
    return IOStatus::OK();
  }

  IOStatus LockFile(const std::string& fname, const IOOptions& /*opts*/,
                    FileLock** lock, IODebugContext* /*dbg*/) override {
    std::cout << "LockFile has not been implemented" << std::endl;
    return IOStatus::OK();
  }

  IOStatus UnlockFile(FileLock* lock, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    std::cout << "UnlockFile has not been implemented" << std::endl;
    return IOStatus::OK();
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*opts*/, std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    if (!db_path.empty() && db_path[0] == '/') {
      *output_path = db_path;
      return IOStatus::OK();
    }

    char the_path[256];
    char* ret = getcwd(the_path, 256);
    if (ret == nullptr) {
      return IOStatus::IOError(strerror(errno));
    }

    *output_path = ret;
    return IOStatus::OK();
  }

  IOStatus GetTestDirectory(const IOOptions& /*opts*/, std::string* result,
                            IODebugContext* /*dbg*/) override {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/rocksdbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    {
      IOOptions opts;
      return CreateDirIfMissing(*result, opts, nullptr);
    }
    return IOStatus::OK();
  }

  IOStatus GetFreeSpace(const std::string& fname, const IOOptions& /*opts*/,
                        uint64_t* free_space,
                        IODebugContext* /*dbg*/) override {
    struct statvfs sbuf;

    if (statvfs(fname.c_str(), &sbuf) < 0) {
      return IOError("While doing statvfs", fname, errno);
    }

    *free_space = ((uint64_t)sbuf.f_bsize * sbuf.f_bfree);
    return IOStatus::OK();
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& /*opts*/,
                       bool* is_dir, IODebugContext* /*dbg*/) override {
    // First open
    int fd = -1;
    int flags = cloexec_flags(O_RDONLY, nullptr);
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(path.c_str(), flags);
    }
    if (fd < 0) {
      return IOError("While open for IsDirectory()", path, errno);
    }
    IOStatus io_s;
    struct stat sbuf;
    if (fstat(fd, &sbuf) < 0) {
      io_s = IOError("While doing stat for IsDirectory()", path, errno);
    }
    close(fd);
    if (io_s.ok() && nullptr != is_dir) {
      *is_dir = S_ISDIR(sbuf.st_mode);
    }
    return io_s;
  }

  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                 const DBOptions& db_options) const override {
    FileOptions optimized = file_options;
    optimized.use_mmap_writes = false;
    optimized.use_direct_writes = false;
    optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
    // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
    // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
    // test and make this false
    optimized.fallocate_with_keep_size = true;
    optimized.writable_file_max_buffer_size =
        db_options.writable_file_max_buffer_size;
    return optimized;
  }

  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    FileOptions optimized = file_options;
    optimized.use_mmap_writes = false;
    optimized.use_direct_writes = false;
    optimized.fallocate_with_keep_size = true;
    return optimized;
  }
#ifdef OS_LINUX
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return logical_block_size_cache_.RefAndCacheLogicalBlockSize(paths);
  }
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    logical_block_size_cache_.UnrefAndTryRemoveCachedLogicalBlockSize(paths);
    return Status::OK();
  }
#endif
 private:
//  bool checkedDiskForMmap_;
//  bool forceMmapOff_;  // do we override Env options?
  RDMA_Manager* rdma_mg_;
  std::map<std::string, SST_Metadata*> file_to_sst_meta;
  std::unordered_map<ibv_mr*, In_Use_Array>* Remote_Bitmap;
  std::unordered_map<ibv_mr*, In_Use_Array>* Local_Bitmap;

  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname) {
    struct stat statbuf;
    if (stat(dname.c_str(), &statbuf) == 0) {
      return S_ISDIR(statbuf.st_mode);
    }
    return false;  // stat() failed return false
  }

  bool SupportsFastAllocate(const std::string& path) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
    struct statfs s;
    if (statfs(path.c_str(), &s)) {
      return false;
    }
    switch (s.f_type) {
      case EXT4_SUPER_MAGIC:
        return true;
      case XFS_SUPER_MAGIC:
        return true;
      case TMPFS_MAGIC:
        return true;
      default:
        return false;
    }
#else
    (void)path;
    return false;
#endif
  }

#if defined(ROCKSDB_IOURING_PRESENT)
  // io_uring instance
  std::unique_ptr<ThreadLocalPtr> thread_local_io_urings_;
#endif

//  size_t page_size_;

  // If true, allow non owner read access for db files. Otherwise, non-owner
  //  has no access to db files.
  bool allow_non_owner_access_;

#ifdef OS_LINUX
  static LogicalBlockSizeCache logical_block_size_cache_;
#endif
  static size_t GetLogicalBlockSize(const std::string& fname, int fd);
  // In non-direct IO mode, this directly returns kDefaultPageSize.
  // Otherwise call GetLogicalBlockSize.
  static size_t GetLogicalBlockSizeForReadIfNeeded(const EnvOptions& options,
                                                   const std::string& fname,
                                                   int fd);
  static size_t GetLogicalBlockSizeForWriteIfNeeded(const EnvOptions& options,
                                                    const std::string& fname,
                                                    int fd);
};

#ifdef OS_LINUX
LogicalBlockSizeCache RDMAFileSystem::logical_block_size_cache_;
#endif

//size_t RDMAFileSystem::GetLogicalBlockSize(const std::string& fname, int fd) {
//#ifdef OS_LINUX
//  return logical_block_size_cache_.GetLogicalBlockSize(fname, fd);
//#else
//  (void)fname;
//  return PosixHelper::GetLogicalBlockSizeOfFd(fd);
//#endif
//}

size_t RDMAFileSystem::GetLogicalBlockSizeForReadIfNeeded(
    const EnvOptions& options, const std::string& fname, int fd) {
  return kDefaultPageSize;
}

//size_t RDMAFileSystem::GetLogicalBlockSizeForWriteIfNeeded(
//    const EnvOptions& options, const std::string& fname, int fd) {
//  return kDefaultPageSize;
//}

RDMAFileSystem::RDMAFileSystem()
    :
      allow_non_owner_access_(true) {
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */
      -1, /* gid_idx */
      4*10*1024*1024 /*initial local buffer size*/
  };
  Remote_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
  Local_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
  rdma_mg_ = new RDMA_Manager(config, Remote_Bitmap, Local_Bitmap);
  rdma_mg_->Set_Up_RDMA();
#if defined(ROCKSDB_IOURING_PRESENT)
  // Test whether IOUring is supported, and if it does, create a managing
  // object for thread local point so that in the future thread-local
  // io_uring can be created.
  struct io_uring* new_io_uring = CreateIOUring();
  if (new_io_uring != nullptr) {
    thread_local_io_urings_.reset(new ThreadLocalPtr(DeleteIOUring));
    delete new_io_uring;
  }
#endif
}
rocksdb::RDMAFileSystem::~RDMAFileSystem() {
  delete Remote_Bitmap;
  delete Local_Bitmap;
  delete rdma_mg_;
}

}  // namespace

//
// Default Posix FileSystem
//
std::shared_ptr<FileSystem> FileSystem::Default() {
  static RDMAFileSystem default_fs;
  static std::shared_ptr<RDMAFileSystem> default_fs_ptr(
      &default_fs, [](RDMAFileSystem*) {});
  return default_fs_ptr;
}

#ifndef ROCKSDB_LITE
static FactoryFunc<FileSystem> posix_filesystem_reg =
    ObjectLibrary::Default()->Register<FileSystem>(
        "posix://.*",
        [](const std::string& /* uri */, std::unique_ptr<FileSystem>* f,
           std::string* /* errmsg */) {
          f->reset(new RDMAFileSystem());
          return f->get();
        });
#endif

}  // namespace ROCKSDB_NAMESPACE

#endif
