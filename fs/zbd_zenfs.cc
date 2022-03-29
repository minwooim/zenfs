// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <time.h>
#include <chrono>
#include <cmath>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "logging/logging.h"
#include "util/aligned_buffer.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

extern std::shared_ptr<Logger> _logger;

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      busy_(false),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      wp_before_finish_(wp_),
      finished_(false) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));

  extent_start_ = start_;
  id_ = start_ / zbd_zone_len(z);
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

IOStatus Zone::CloseWR() {
  assert(IsBusy());

  IOStatus status = Close();

  if (capacity_ == 0) zbd_->NotifyIOZoneFull();

  return status;
}

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  ROCKS_LOG_INFO(_logger, "reset zone %ld", GetZoneId());
  ret = zbd_reset_zones(zbd_->GetWriteFD(), start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(zbd_->GetReadFD(), start_, zone_sz, ZBD_RO_ALL, &z,
                         &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(&z);

  wp_ = start_;
  wp_before_finish_ = start_;
  extent_start_ = start_;

  if (GetZoneId() >= ZSG_START_ZONE) {
    zbd_->free_zones_[level_]->push(this);
  }

  finished_ = false;
  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  ROCKS_LOG_INFO(_logger, "zone %ld finished", GetZoneId());
  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_before_finish_ = wp_;
  wp_ = start_ + zone_sz;
  finished_ = true;

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

  ROCKS_LOG_INFO(_logger, "Zone::Finish(): Close zone[%ld]",
        this->start_ / this->max_capacity_);

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int fd = zbd_->GetWriteFD();
  int ret;
  // NOWS: Namespace Optimal Write Size
  uint32_t unit = 128 * KB;

  if (capacity_ < size) {
    ROCKS_LOG_ERROR(_logger, "zone append failed. zone %ld capacity full, cap=%ld, size=%u",
        GetZoneId(), capacity_, size);
    return IOStatus::NoSpace("Not enough capacity for append");
  }

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    unit = (unit > left) ? left : unit;
    ret = pwrite(fd, ptr, unit, wp_);
    if (ret < 0) return IOStatus::IOError("Write failed");
    assert(ret == (int) unit);

    ptr += ret;
    wp_ += ret;
    wp_before_finish_ += ret;
    capacity_ -= ret;
    left -= ret;
  }

  return IOStatus::OK();
}

inline IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : filename_("/dev/" + bdevname), logger_(logger), metrics_(metrics) {
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());

  active_zones_ = 0;
  CK_BITMAP_INIT(&zone_bitmap_, ZSG_NR_ZONES, 0);
  for (int i = 0; i < ZSG_MAX_ACTIVE_ZONES; i++) {
    zone_tokens_.push(true);
  }

  // We keep the zone level list for level + 1 for free zones spare
  for (int l = 0; l < ZSG_NR_LEVELS + 1; l++) {
    free_zones_.push_back(new tbb::concurrent_queue<Zone*>);
    partial_zones_.push_back(new tbb::concurrent_queue<Zone*>);
  }
}

std::string ZonedBlockDevice::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

IOStatus ZonedBlockDevice::CheckScheduler() {
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;

  s.erase(0, 5);  // Remove "/dev/" from /dev/nvmeXnY
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    return IOStatus::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);
  if (buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return IOStatus::InvalidArgument(
        "Current ZBD scheduler is not mq-deadline, set it to mq-deadline.");
  }

  f.close();
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  struct zbd_zone *zone_rep;
  unsigned int reported_zones;
  uint64_t addr_space_sz;
  zbd_info info;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  int ret;

  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  /* The non-direct file descriptor acts as an exclusive-use semaphore */
  if (exclusive) {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_EXCL, &info);
  } else {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  }

  if (read_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for read: " + ErrorToString(errno));
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for direct read: " +
        ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument(
          "Failed to open zoned block device for write: " +
          ErrorToString(errno));
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK()) return ios;

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

  /* We need one open zone for meta data writes, the rest can be used for files
   */
  if (info.max_nr_active_zones == 0)
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones - 1;

  if (info.max_nr_open_zones == 0)
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones - 1;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       info.nr_zones, info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        meta_zones.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }

        io_zones.push_back(newZone);

        if (newZone->GetZoneId() >= ZSG_START_ZONE &&
            newZone->GetZoneId() < ZSG_NR_ZONES) {
          free_zones_[ZSG_NR_LEVELS]->push(newZone);
        }

        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
            zbd_zone_closed(z)) {
          active_io_zones_++;
          if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        IOStatus status = newZone->CheckRelease();
        if (!status.ok()) return status;
      }
    }
  }

  free(zone_rep);
  start_time_ = time(NULL);
  return IOStatus::OK();
}

void ZonedBlockDevice::NotifyIOZoneFull() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  active_io_zones_--;
  zone_resources_.notify_one();
}

void ZonedBlockDevice::NotifyIOZoneClosed() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  open_io_zones_--;
  zone_resources_.notify_one();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  io_zones_mtx.lock();

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());

  io_zones_mtx.unlock();
}

void ZonedBlockDevice::LogZoneUsage() {
  int cnt = 0;
  int partial_cnt = 0;

  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0 && !z->capacity_) {
      Debug(logger_, "Zone[%ld] full, 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->GetZoneId(), z->start_, used, used / MB);
      cnt++;
    } else if (used > 0) {
      Debug(logger_, "Zone[%ld] partial, 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->GetZoneId(), z->start_, used, used / MB);
      partial_cnt++;
    }
  }

  Debug(logger_, "Number of used (finished) zones: %d, partial zones: %d",
      cnt, partial_cnt);
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);
}

#define LIFETIME_DIFF_NOT_GOOD (100)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

Status ZonedBlockDevice::ResetUnusedIOZones() {
  /* Reset any unused zones */
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsUsed() && !z->IsEmpty()) {
        if (!z->IsFull()) active_io_zones_--;
        if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
      }
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
    }
  }
  return Status::OK();
}

IOStatus ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint file_lifetime,
                                        Zone **out_zone) {
  Zone *allocated_zone = nullptr;
  Zone *finish_victim = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;
  bool ok = false;
  (void)ok;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_IO_ALLOC_NON_WAL_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  *out_zone = nullptr;

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  io_zones_mtx.lock();

  /* Make sure we are below the zone open limit */
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this] {
      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
      return false;
    });
  }

  /* Reset any unused zones and finish used zones under capacity treshold*/
  for (const auto z : io_zones) {
    if (z->id_ >= ZSG_START_ZONE) {
      break;
    }

    if (!z->Acquire()) {
      Debug(
          _logger,
          "ZonedBlockDevice::AllocateZone(): Failed to acquire. skip zone[%ld]",
          z->start_ / z->max_capacity_);
      continue;
    }

    if (z->IsEmpty() || (z->IsFull() && z->IsUsed())) {
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
      continue;
    }

    if (!z->IsUsed()) {
      if (!z->IsFull()) active_io_zones_--;

      Debug(_logger,
            "ZonedBlockDevice::AllocateZone(): Not used zone. reset zone[%ld], "
            "used_capacity_=0x%lx",
            z->start_ / z->max_capacity_, z->used_capacity_.load());
      s = z->Reset();
      if (!s.ok()) {
        Debug(logger_, "Failed resetting zone !");
        return s;
      }

      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
      continue;
    }

    if ((z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100))) {
      /* If there is less than finish_threshold_% remaining capacity in a
       * non-open-zone, finish the zone */
      s = z->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
        return s;
      }
      active_io_zones_--;
    }

    if (!z->IsFull()) {
      if (finish_victim == nullptr) {
        finish_victim = z;
      } else if (finish_victim->capacity_ > z->capacity_) {
        IOStatus status = finish_victim->CheckRelease();
        if (!status.ok()) return status;
        finish_victim = z;
      } else {
        IOStatus status = z->CheckRelease();
        if (!status.ok()) return status;
      }
    } else {
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
    }
  }

  // Holding finish_victim if != nullptr

  /* Try to fill an already open zone(with the best life time diff) */
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if ((z->used_capacity_ > 0) && !z->IsFull()) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        if (diff <= best_diff) {
          if (allocated_zone != nullptr) {
            IOStatus status = allocated_zone->CheckRelease();
            if (!status.ok()) return status;
          }
          allocated_zone = z;
          best_diff = diff;
        } else {
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
        }
      } else {
        IOStatus status = z->CheckRelease();
        if (!status.ok()) return status;
      }
    }
  }

  // Holding finish_victim if != nullptr
  // Holding allocated_zone if != nullptr

  /* If we did not find a good match, allocate an empty one */
  if (best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    /* If we at the active io zone limit, finish an open zone(if available) with
     * least capacity left */
    if (active_io_zones_.load() == max_nr_active_io_zones_ &&
        finish_victim != nullptr) {
      s = finish_victim->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
        return s;
      }
      active_io_zones_--;
    }

    if (active_io_zones_.load() < max_nr_active_io_zones_) {
      for (const auto z : io_zones) {
        if (z->Acquire()) {
          if (z->IsEmpty()) {
            z->lifetime_ = file_lifetime;
            if (allocated_zone != nullptr) {
              IOStatus status = allocated_zone->CheckRelease();
              if (!status.ok()) return status;
            }
            allocated_zone = z;
            active_io_zones_++;
            new_zone = 1;
            break;
          } else {
            IOStatus status = z->CheckRelease();
            if (!status.ok()) return status;
          }
        }
      }
    }
  }

  if (finish_victim != nullptr) {
    IOStatus status = finish_victim->CheckRelease();
    if (!status.ok()) return status;
    finish_victim = nullptr;
  }

  if (allocated_zone) {
    ok = allocated_zone->IsBusy();
    assert(ok);
    open_io_zones_++;
    Debug(logger_,
          "Allocating zone[%ld](new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: "
          "%d, diff=%d\n",
          allocated_zone->start_ / allocated_zone->max_capacity_, new_zone,
          allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime,
          allocated_zone->lifetime_ != file_lifetime);
  }

  io_zones_mtx.unlock();
  LogZoneStats();

  *out_zone = allocated_zone;

  metrics_->ReportGeneral(ZENFS_OPEN_ZONES, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES, active_io_zones_);

  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }

uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto &zone : io_zones) snapshot.emplace_back(*zone);
}

bool ZonedBlockDevice::GetPartialZone(Zone*& zone, ZoneFile* zonefile) {
  if (zonefile->zones_.try_pop(zone)) {
    return true;
  }

  return false;
}

bool ZonedBlockDevice::GetPartialZone(Zone*& zone, int level) {
  if (partial_zones_[level]->try_pop(zone)) {
    return true;
  }

  return false;
}

bool ZonedBlockDevice::GetFreeZoneFromSpare(Zone*& zone, int from_level) {
  const int spare = ZSG_NR_LEVELS;
  if (free_zones_[spare]->try_pop(zone)) {
    // When this zone needs to be pushed to other queue, it should follow
    // the level given.
    zone->level_ = from_level;
    zone->lifetime_ = LevelToLifetime(from_level);
    active_zones_++;
    return true;
  }

  return false;
}

bool ZonedBlockDevice::GetFreeZone(Zone*& zone, int level) {
  if (free_zones_[level]->try_pop(zone)) {
    active_zones_++;
    return true;
  }

  // Borrow from the spare list or other level's queue
  return GetFreeZoneFromSpare(zone, level);
}

bool ZonedBlockDevice::AllocateZSGZone(Zone*& zone, ZoneFile* zonefile) {
  bool token;
  Env::WriteLifeTimeHint lifetime = zonefile->GetWriteLifeTimeHint();
  int level = LifetimeToLevel(lifetime);

  // First, we need to get a zone from same zonefile zones as possible as we
  // can to utilize space effectively.
  if (GetPartialZone(zone, zonefile)) {
    return true;
  }

  if (GetPartialZone(zone, level)) {
    return true;
  }

  if (!zone_tokens_.try_pop(token)) {
    return false;
  }

  if (!GetFreeZone(zone, level)) {
    zone_tokens_.push(token);
    return false;
  }

  zonefile->nr_zones_++;
  ROCKS_LOG_INFO(_logger, "%s(level=%d): Free zone allocated, file_zones_=%d, active_zones_=%d, tokens=%ld",
                 zonefile->GetFilename().c_str(), level, zonefile->nr_zones_.load(),
                 active_zones_.load(), zone_tokens_.unsafe_size());
  return true;
}

static void BGWorkAppend(char *data, size_t size,
                         Zone *zone, ZoneFile* zonefile, AlignedBuffer *buf,
                         size_t file_advance, size_t leftover_tail);

void ZoneStripingGroup::Append(ZoneFile *zonefile, void *data, size_t size,
                               IODebugContext *dbg) {
  const size_t block_size = zbd_->GetBlockSize();
  size_t each = (size < ZSG_ZONE_SIZE) ? size : ZSG_ZONE_SIZE;
  size_t aligned = (each + (block_size - 1)) & ~(block_size - 1);
  char *_data = (char *) data;
  Zone* z;

  while (!zbd_->AllocateZSGZone(z, zonefile)) {
    ;
  }
  ROCKS_LOG_INFO(_logger, "%s(level=%d): Zone allocated, zoneid=%ld, current_active_zones=%d",
                 zonefile->GetFilename().c_str(),
                 LifetimeToLevel(zonefile->GetWriteLifeTimeHint()),
                 z->GetZoneId(),
                 zbd_->active_zones_.load());

  zonefile->PushExtent(new ZoneExtent(z->extent_start_, aligned, z));
  z->extent_start_ = z->wp_ + aligned;
  z->used_capacity_ += aligned;
  thread_pool_.push_back(std::thread(BGWorkAppend, _data,
                                     aligned, z, zonefile,
                                     static_cast<AlignedBuffer*>(dbg->buf_),
                                     dbg->file_advance_, dbg->leftover_tail_));
}

static void BGWorkAppend(char *data, size_t size,
                         Zone *zone, ZoneFile* zonefile, AlignedBuffer *buf,
                         size_t file_advance, size_t leftover_tail) {
  IOStatus s;

  s = zone->Append(data, size);
  if (!s.ok()) {
    printf("pwrite() failed, zone=%ld\n", zone->GetZoneId());
    abort();
  }

  buf->RefitTail(file_advance, leftover_tail);
  delete buf->Release();

  if (zone->capacity_ < ZSG_ZONE_SIZE) {
    zone->Finish();
    zone->zbd_->active_zones_--;
    zone->zbd_->zone_tokens_.push(true);
    ROCKS_LOG_INFO(_logger, "active: Zone finished, active_zones_=%d, tokens=%ld",
                   zone->zbd_->active_zones_.load(),
                   zone->zbd_->zone_tokens_.unsafe_size());
  } else {
    zonefile->zones_.push(zone);
  }
}

void ZoneStripingGroup::Fsync(ZoneFile* zonefile) {
  if (GetState() == ZSGState::kFull) {
    return;
  }

  for (auto& thread : thread_pool_) {
    thread.join();
  }
  thread_pool_.clear();

  SetState(ZSGState::kFull);

  Zone* z;
  ZonedBlockDevice* zbd = zonefile->GetZbd();
  while (zonefile->zones_.try_pop(z)) {
    if (z->capacity_ < ZSG_ZONE_SIZE) {
      z->Finish();
      zbd->active_zones_--;
      zbd->zone_tokens_.push(true);
    } else {
      zbd->partial_zones_[zonefile->Level()]->push(z);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
