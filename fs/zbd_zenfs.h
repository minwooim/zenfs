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
#include <ck_bitmap.h>

// Number of zones being striped for a SSTable
#define ZSG_ZONES         (11)
#define ZSG_LAST_ZONE     (Rounddown(40700, ZSG_ZONES))
// Number of SSTables in a single zone.
#define ZSG_FILES         (1)
// Actual size of a SSTable
#define ZSG_START_ZONE    (ZSG_ZONES * 5)

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
};

enum class ZSGState {
  kEmpty              = 0,
  kFull               = 3,
};

class ZoneStripingGroup {
 public:
  ZonedBlockDevice *zbd_;
  ZSGState state_;
  int nr_zones_;
  int id_;
  int current_nr_zones_;
  std::vector<Zone *> zones_;
  std::shared_ptr<Logger> logger_;

  std::vector<std::thread> thread_pool_;

  // Number of current SST files written

  // Two cases where this variable is updated:
  //   (1) ZonedBlockDevice::AllocateZoneStripingGroup
  //   (2) ZoneFile::~ZoneFile()
  int current_sst_files_;
  // Number of total SSTables whether or not it's removed.  This value will not
  // be reset until this group is destroyed (all SSTables are removed).
  int total_sst_files_;

  int current_zone_;

  // Available zone list
  CK_BITMAP_INSTANCE(ZSG_ZONES) used_bitmap_;

  ZoneStripingGroup(ZonedBlockDevice *zbd, int nr_zones, int id,
      std::shared_ptr<Logger> logger) {
    zbd_ = zbd;
    state_ = ZSGState::kEmpty;
    nr_zones_ = nr_zones;
    id_ = id;
    current_nr_zones_ = 0;
    zones_.resize(nr_zones);
    logger_ = logger;

    thread_pool_.reserve(nr_zones);

    current_sst_files_ = 0;
    total_sst_files_ = 0;

    current_zone_ = 0;

    CK_BITMAP_INIT(&used_bitmap_, ZSG_ZONES, 0);
  }

  ~ZoneStripingGroup();

  uint64_t GetId() {
    return id_;
  }

  int GetNumZones() {
    return nr_zones_;
  }

  Zone *GetZone(int id) {
    return zones_[id];
  }

  void AddZone(Zone *zone) {
    zones_[current_nr_zones_++] = zone;
    Info(logger_, "zsg[%d] (state=%d): add zone[%ld] (start=0x%lx)",
        id_, (int)state_, zone->GetZoneId(), zone->GetStartLBA());
  }

  bool IsFull() {
    return ZSG_FILES == total_sst_files_;
  }

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
  void PushExtents(ZoneFile *zonefile);
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
  tbb::concurrent_queue<ZoneStripingGroup*> zsgq_;
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

  ZoneStripingGroup *AllocateZoneStripingGroup();

  inline void PushToZSGQ(ZoneStripingGroup *zsg) {
    zsgq_.push(zsg);
  };

  // number of active zones (open, closed) within ZSG
  std::mutex zsgq_push_mtx_;
  std::mutex zsgpq_push_mtx_;
  std::mutex zsgq_pop_mtx_;

 private:
  std::string ErrorToString(int err);
  IOStatus GetZoneDeferredStatus();
};

}  // namespace ROCKSDB_NAMESPACE
