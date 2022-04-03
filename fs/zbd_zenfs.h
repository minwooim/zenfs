// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <queue>

#include <tbb/concurrent_queue.h>

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/file_system.h"

// We should include port/port_posix.h for compatibiltiy, but we don't include
// port/ directory in this header file.
#include <thread>

#define ZSG_NR_ZONES      (40704)
// Size of a buffer for a zone striping group
#define ZSG_ZONE_SIZE     (1ULL * 1024 * 1024)
// Actual size of a SSTable
#define ZSG_START_ZONE    (16)
#define ZSG_MAX_ACTIVE_ZONES  (256)

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;
class ZoneSnapshot;
class ZoneFile;
class ZoneStripingGroup;

class Zone {
 public:
  ZonedBlockDevice *zbd_;
  std::atomic_bool busy_;

  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<long> used_capacity_;

  // This is only for ZSG.
  // We can have this here in Zone because a single zone can have exactly
  // N SST files.
  uint64_t extent_start_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);

  IOStatus CloseWR(); /* Done writing */

  inline IOStatus CheckRelease();

  int id_;
  inline uint64_t GetZoneId() {
    return id_;
  }

  inline uint64_t GetStartLBA() {
    // XXX: Should get LBA size of this block device
    return start_ / 4096;
  }

  inline uint64_t GetWritePointer() {
    return wp_;
  }

  uint64_t wp_before_finish_;
  bool finished_;
};

enum class ZSGState {
  kEmpty              = 0,
  kFull               = 3,
};

class ZoneStripingGroup {
 public:
  ZonedBlockDevice *zbd_;
  ZSGState state_;
  std::vector<std::thread> thread_pool_;

  ZoneStripingGroup(ZonedBlockDevice *zbd) {
    zbd_ = zbd;
    state_ = ZSGState::kEmpty;
  }

  ~ZoneStripingGroup();

  ZSGState GetState() {
    return state_;
  }

  void SetState(ZSGState state) {
    switch (state_) {
      case ZSGState::kEmpty:
        if (state == ZSGState::kFull) {
          break;
        }
        [[fallthrough]];
      case ZSGState::kFull:
        if (state == ZSGState::kEmpty) {
          break;
        }
        [[fallthrough]];
      default:
        assert(false);
        return;
    }

    state_ = state;
  }

  // IOStatus BGWorkAppend(int i, char *data, size_t size);
  void Append(ZoneFile *zonefile, void *data, size_t size, IODebugContext *dbg);
  void Fsync(ZoneFile *zonefile);
};

template <typename T>
class ZoneStripingGroupQueue {
  public:
    std::mutex mtx;
    std::queue<T> queue;

    void push(T const & value) {
      mtx.lock();
      queue.push(value);
      mtx.unlock();
    }

    bool pop(T & value) {
      mtx.lock();
      if (queue.empty()) {
        mtx.unlock();
        return false;
      }

      value = queue.front();
      queue.pop();
      mtx.unlock();

      return true;
    }
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::mutex io_zones_mtx;
  std::vector<Zone *> meta_zones;
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  std::shared_ptr<ZenFSMetrics> metrics_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);
  IOStatus CheckScheduler();

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateZone(Env::WriteLifeTimeHint file_lifetime, Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockSize();

  Status ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();

  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint64_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void NotifyIOZoneFull();
  void NotifyIOZoneClosed();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  bool GetZone(Zone* z);
  void PutZone(Zone* z);
  bool BusyZone(Zone* z);

  bool AllocateZSGZone(ZoneFile* zonefile, Zone*& zone);
  bool GetPartialZone(Zone*& zone);
  bool GetFreeZone(Zone*& zone);

 private:
  std::string ErrorToString(int err);
  IOStatus GetZoneDeferredStatus();

 public:
  std::atomic<int> active_zones_;
  tbb::concurrent_queue<bool> zone_tokens_;
  tbb::concurrent_queue<Zone*> free_zones_;
  tbb::concurrent_queue<Zone*> partial_zones_;
};

}  // namespace ROCKSDB_NAMESPACE
