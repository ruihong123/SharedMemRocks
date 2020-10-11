//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/random_access_file_reader.h"

#include <algorithm>
#include <mutex>

#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

Status RandomAccessFileReader::Read(const IOOptions& opts, uint64_t offset,
                                    size_t n, Slice* result, char* scratch,
                                    AlignedBuf* aligned_buf,
                                    bool for_compaction) const {

  Status s;
  s = file_->Read(offset, n, opts, result, scratch, nullptr);




  return s;
}

size_t End(const FSReadRequest& r) {
  return static_cast<size_t>(r.offset) + r.len;
}

FSReadRequest Align(const FSReadRequest& r, size_t alignment) {
  FSReadRequest req;
  req.offset = static_cast<uint64_t>(
    TruncateToPageBoundary(alignment, static_cast<size_t>(r.offset)));
  req.len = Roundup(End(r), alignment) - req.offset;
  req.scratch = nullptr;
  return req;
}

bool TryMerge(FSReadRequest* dest, const FSReadRequest& src) {
  size_t dest_offset = static_cast<size_t>(dest->offset);
  size_t src_offset = static_cast<size_t>(src.offset);
  size_t dest_end = End(*dest);
  size_t src_end = End(src);
  if (std::max(dest_offset, src_offset) > std::min(dest_end, src_end)) {
    return false;
  }
  dest->offset = static_cast<uint64_t>(std::min(dest_offset, src_offset));
  dest->len = std::max(dest_end, src_end) - dest->offset;
  return true;
}

Status RandomAccessFileReader::MultiRead(const IOOptions& opts,
                                         FSReadRequest* read_reqs,
                                         size_t num_reqs,
                                         AlignedBuf* aligned_buf) const {

  Status s;

  s = file_->MultiRead(read_reqs, num_reqs, opts, nullptr);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
