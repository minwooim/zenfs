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
      wp_(zbd_zone_wp(z)) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));

  Debug(_logger, "Zone::Zone(): zone[%ld] start_=0x%lx, capacity_=0x%lx",
        this->start_ / this->max_capacity_, this->start_,
        this->capacity_);

  extent_start_ = start_;
  id_ = start_ / max_capacity_;
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
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_ = start_ + zone_sz;

  ROCKS_LOG_INFO(_logger, "Zone::Finish(): Finish zone[%ld]",
        this->start_ / this->max_capacity_);

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
    ROCKS_LOG_ERROR(_logger, "zone %ld capacity full", GetZoneId());
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

  nr_active_zsgs_ = 0;
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

  ZoneStripingGroup *zsg = nullptr;

  for (int j = 0; i < reported_zones; i++,j++) {
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

        // XXX: remove temporary condition for start zone
        if (j >= ZSG_START_ZONE) {
        if (!(j & (ZSG_ZONES - 1))) {
            int id = j / ZSG_ZONES;
            zsg = new ZoneStripingGroup(this, ZSG_ZONES, id, logger_);
            PushToZSGQ(zsg);
            zsg->AddZone(newZone);
        } else {
            zsg->AddZone(newZone);
        }
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
    if (!z->Acquire()) {
      Debug(
          _logger,
          "ZonedBlockDevice::AllocateZone(): Failed to acquire. skip zone[%ld]",
          z->start_ / z->max_capacity_);
      continue;
    }

    // 이 부분부터는 해당 zone (z) 가 busy_ = true 되어 있는 상태!
    // (1) Empty zone 인 경우 (start == wp), busy_ = false 로 busy clear 함!
    // (2) Full 이고 Used 인 경우 (진짜 full), 동일하게 busy clearing!
    //     -> Full 이고 Used 가 아닌 경우, cap 자체가 0 인 zone... ? (애초부터
    //     사용 불가 존 ?)
    if (z->IsEmpty() || (z->IsFull() && z->IsUsed())) {
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
      continue;
    }

    // "zone 에 있는 파일들이 모두 지워진 경우"
    // 여기서부턴 조금이라도 쓰인 (partially written) zone 의 경우만 해당됨.
    // 하지만 !IsUsed() 인 경우는 ZoneFile::~ZoneFile() 이 불려서 used_capacity_
    // 가 0 이 된 경우 즉, 다 사용한 file 로 invalid 한 file 을 의미함.
    if (!z->IsUsed()) {
      // capacity_ == 0 인 경우를 의미함
      if (!z->IsFull()) active_io_zones_--;

      // 이 경우는 쓰다가 rename file, delete file 혹은 ~ZoneFile() 을 만나서
      // free 된 경우
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

    // // 일정 수준 threshold 아래로 내려간 존은 finish 해서 full 상태로 만듬
    // // (wp 를 zone size 로 맞추고, capacity_ == 0)
    // if ((z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100))) {
    //   /* If there is less than finish_threshold_% remaining capacity in a
    //    * non-open-zone, finish the zone */
    //   s = z->Finish();
    //   if (!s.ok()) {
    //     Debug(logger_, "Failed finishing zone");
    //     return s;
    //   }
    //   active_io_zones_--;
    // }

  //   if (!z->IsFull()) {
  //     if (finish_victim == nullptr) {
  //       finish_victim = z;
  //     } else if (finish_victim->capacity_ > z->capacity_) {
  //       // finish_victim 보다 더 적은 양이 남은 zone 이 있다면, 그 zone 이
  //       // 결국에 finish_victim 이 됨 그 전에 finish_victim 을 busy 에서
  //       // 풀어주고..
  //       IOStatus status = finish_victim->CheckRelease();
  //       if (!status.ok()) return status;
  //       finish_victim = z;
  //     } else {
  //       IOStatus status = z->CheckRelease();
  //       if (!status.ok()) return status;
  //     }
  //   } else {
  //     IOStatus status = z->CheckRelease();
  //     if (!status.ok()) return status;
  //   }
  }

  // Holding finish_victim if != nullptr

  /* Try to fill an already open zone(with the best life time diff) */
  // for (const auto z : io_zones) {
  //   if (z->Acquire()) {
  //     // 이미 open 된 zone 을 재사용하기 위해
  //     if ((z->used_capacity_ > 0) && !z->IsFull()) {
  //       unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
  //       if (diff <= best_diff) {
  //         if (allocated_zone != nullptr) {
  //           IOStatus status = allocated_zone->CheckRelease();
  //           if (!status.ok()) return status;
  //         }
  //         allocated_zone = z;
  //         best_diff = diff;
  //       } else {
  //         IOStatus status = z->CheckRelease();
  //         if (!status.ok()) return status;
  //       }
  //     } else {
  //       IOStatus status = z->CheckRelease();
  //       if (!status.ok()) return status;
  //     }
  //   }
  // }

  // Holding finish_victim if != nullptr
  // Holding allocated_zone if != nullptr

  /* If we did not find a good match, allocate an empty one */
  if (best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    /* If we at the active io zone limit, finish an open zone(if available) with
     * least capacity left */
    // if (active_io_zones_.load() == max_nr_active_io_zones_ &&
    //     finish_victim != nullptr) {
    //   s = finish_victim->Finish();
    //   if (!s.ok()) {
    //     Debug(logger_, "Failed finishing zone");
    //     return s;
    //   }
    //   active_io_zones_--;
    // }

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

ZoneStripingGroup *ZonedBlockDevice::AllocateZoneStripingGroup(int *id) {
  ZoneStripingGroup *zsg;
  bool wait;

  zsgq_pop_mtx_.lock();
 again:
  wait = false;
  if (nr_active_zsgs_.load() == ZSG_MAX_OPEN_GROUP) {
    wait = true;
    while (true) {
      if (!zsgpq_.empty()) {
        break;
      }
    }
  }

  if (!zsgpq_.empty()) {
    zsg = zsgpq_.front();
    zsgpq_.pop();
  } else if (!zsgq_.empty()) {
    // XXX: Don't know why this happens here even we checked the zsgpq_ is not
    // empty up there, but it reaches here.  So, let's get this up to there.
    if (wait) {
      goto again;
    }
    zsg = zsgq_.front();
    zsgq_.pop();
    nr_active_zsgs_++;
  } else {
    return nullptr;
  }
  assert(nr_active_zsgs_ <= ZSG_MAX_OPEN_GROUP);
  zsgq_pop_mtx_.unlock();

  assert(zsg->GetState() == ZSGState::kEmpty ||
          zsg->GetState() == ZSGState::kPartialIdle);

  zsg->SetState(ZSGState::kPartialInprogress);

  *id = zsg->current_sst_files_++;
  zsg->total_sst_files_++;
  ROCKS_LOG_INFO(_logger, "zsg %d, first zone %ld, total_sst_files_=%d",
      zsg->id_, zsg->zones_[0]->GetZoneId(), zsg->total_sst_files_);

  return zsg;
}

void ZoneStripingGroup::Append(int id, void *data, size_t size) {
  if (!buffers_[id]) {
    buffers_[id] = new AlignedBuffer;
    buffers_[id]->Alignment(4096);
    buffers_[id]->AllocateNewBuffer(ZSG_BUFFER_SIZE);
  }

  // First, just buffering the data from the upper layer
  buffers_[id]->Append((char *) data, size);
}

static void BGWorkAppend(char *data, size_t size, Zone *zone) {
  IOStatus s;

  s = zone->Append(data, size);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(_logger, " zone %ld append pwrite failed, nr_active_zsgs=%d",
        zone->GetZoneId(), zone->zbd_->nr_active_zsgs_.load());
  }
}

void ZonedBlockDevice::PushToZSGPartialQueue(ZoneStripingGroup *zsg) {
  zsgpq_push_mtx_.lock();
  assert(zsgpq_.size() < ZSG_MAX_OPEN_GROUP);
  zsgpq_.push(zsg);
  zsgpq_push_mtx_.unlock();
}

void ZoneStripingGroup::Fsync(ZoneFile *zonefile, int id) {
  if (!buffers_[id]) {
    return;
  }

  size_t size = buffers_[id]->CurrentSize();
  size_t left = size;
  const size_t each = left / nr_zones_;
  char *data = buffers_[id]->BufferStart();
  const uint32_t block_size = zbd_->GetBlockSize();

  if (!size) {
    return;
  }

  ROCKS_LOG_INFO(_logger, "[ZSG #%d] buffer fsync (id=%d, size=0x%lx)", id_, id, size);

  // Write out the data to the device
  for (int i = 0; i < nr_zones_; i++) {
    size_t per_size = (left > each) ? each: left;
    size_t aligned = (per_size + (block_size - 1)) & ~(block_size - 1);

    thread_pool_.push_back(std::thread(BGWorkAppend, data, aligned, zones_[i]));
    assert(thread_pool_.back().joinable());

    left -= aligned;
    data += aligned;
  }

  for (auto& thread : thread_pool_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  thread_pool_.clear();
  delete buffers_[id]->Release();
  buffers_[id] = nullptr;

  SetState(ZSGState::kPartialIdle);

  PushExtents(zonefile);

  if (!IsFull()) {
    ROCKS_LOG_INFO(logger_, "zsg %d push to partial queue", id_);
    zbd_->PushToZSGPartialQueue(this);
  } else {
    ROCKS_LOG_INFO(logger_, "zsg %d finish zone striping group", id_);
    for (int i = 0; i < nr_zones_; i++) {
      zones_[i]->Finish();
    }
    SetState(ZSGState::kFull);
    zbd_->nr_active_zsgs_--;
  }
}

void ZoneStripingGroup::PushExtents(ZoneFile *zonefile) {
  // Assume that we have finished writing data to zones
  for (int i = 0; i < nr_zones_; i++) {
    Zone *zone = zones_[i];
    size_t length = zone->wp_ - zone->extent_start_;

    if (!length) {
      ROCKS_LOG_WARN(_logger, "length is zero, zone %ld: start=0x%lx, wp=0x%lx, extent_start=0x%lx",
          zone->GetZoneId(), zone->start_, zone->wp_, zone->extent_start_);
      continue;
    }

    zonefile->PushExtent(new ZoneExtent(zone->extent_start_, length, zone));
    zone->used_capacity_ += length;
    zone->extent_start_ = zone->wp_;
  }

  if (zonefile->GetExtents().size() != (size_t) nr_zones_) {
    ROCKS_LOG_WARN(_logger, "%s nr_extents=%d, nr_zones=%d mismatch",
        zonefile->GetFilename().c_str(), (int) zonefile->GetExtents().size(),
        nr_zones_);
  }
}

void ZonedBlockDevice::LogZSGPQ() {
  int nr_partial_zones = 0;

  ROCKS_LOG_INFO(_logger, "Zone Striping Group partial queue");

  for (uint32_t i = 0; i < zsgpq_.size(); i++) {
    ZoneStripingGroup *zsg = zsgpq_.front();
    zsgpq_.pop();

    ROCKS_LOG_INFO(_logger, "[%u] ZSG id=%ld", i, zsg->GetId());
    for (int j = 0; j < zsg->GetNumZones(); j++) {
      ROCKS_LOG_INFO(_logger, "  zone id=%ld",
          zsg->GetZone(j)->GetZoneId());
      nr_partial_zones++;
    }
  }

  ROCKS_LOG_INFO(_logger, "total partial zones %d", nr_partial_zones);
}

void ZonedBlockDevice::LogImmutableGroups() {
  ROCKS_LOG_INFO(_logger, "Logging Immutable Zone Striping Group...");
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
