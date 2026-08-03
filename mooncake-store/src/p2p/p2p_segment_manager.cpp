#include "p2p_segment_manager.h"

#include <glog/logging.h>

#include "master_metric_manager.h"

namespace mooncake {

tl::expected<void, ErrorCode> P2PSegmentManager::MountSegment(
    const Segment& segment) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment.id);
    if (it != mounted_segments_.end()) {
        LOG(WARNING) << "segment_name=" << segment.name
                     << ", warn=segment_already_exists";
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    auto ret = InnerMountSegment(segment);
    if (!ret.has_value()) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment_id=" << segment.id
                   << ", segment_name=" << segment.name
                   << ", segment_size=" << segment.size
                   << ", ret=" << ret.error();
        return ret;
    }
    MasterMetricManager::instance().inc_total_mem_capacity(segment.name,
                                                           segment.size);
    return {};
}

tl::expected<void, ErrorCode> P2PSegmentManager::UnmountSegment(
    const UUID& segment_id) {
    std::string segment_name;
    size_t segment_size = 0;
    {
        // Phase 1: Remove segment under segment_mutex_ (fast).
        SharedMutexLocker lock(&segment_mutex_);
        auto it = mounted_segments_.find(segment_id);
        if (it == mounted_segments_.end()) {
            LOG(WARNING) << "attempt to unmount segment but it does not exist";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        auto ret = OnUnmountSegment(it->second);
        if (!ret.has_value()) {
            LOG(ERROR) << "fail to unmount segment"
                       << ", segment_id=" << segment_id
                       << ", ret=" << ret.error();
            return ret;
        }
        segment_name = it->second->name;
        segment_size = it->second->size;
        mounted_segments_.erase(it);
    }
    // Phase 2: Clean up metadata referencing this segment WITHOUT holding
    // segment_mutex_. This avoids blocking MountSegment/QuerySegments etc.
    if (segment_removal_cb_) {
        segment_removal_cb_(segment_id);
    }
    MasterMetricManager::instance().dec_total_mem_capacity(segment_name,
                                                           segment_size);
    return {};
}

tl::expected<std::vector<Segment>, ErrorCode> P2PSegmentManager::GetSegments() {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    std::vector<Segment> segments;
    for (const auto& entry : mounted_segments_) {
        segments.push_back(*entry.second);
    }
    return segments;
}

tl::expected<std::shared_ptr<Segment>, ErrorCode>
P2PSegmentManager::QuerySegment(const UUID& segment_id) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "QuerySegment: segment not found"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return it->second;
}

void P2PSegmentManager::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_removal_cb_ = std::move(cb);
}

void P2PSegmentManager::SetSegmentChangeCallbacks(
    OnSegmentAddedCallback on_add, OnSegmentRemovedCallback on_remove) {
    on_segment_added_ = std::move(on_add);
    on_segment_removed_ = std::move(on_remove);
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PSegmentManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    bool found = false;
    size_t capacity = 0;
    size_t used = 0;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment) {
            capacity += entry.second->size;
            if (entry.second->IsP2PSegment()) {
                used += entry.second->GetP2PExtra().usage;
            }
            found = true;
            break;
        }
    }

    if (!found) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    return std::make_pair(used, capacity);
}

tl::expected<void, ErrorCode> P2PSegmentManager::InnerMountSegment(
    const Segment& segment) {
    if (!segment.IsP2PSegment()) {
        LOG(ERROR) << "P2PSegmentManager only supports P2PSegmentExtraData";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto new_segment = std::make_shared<Segment>(segment);
    mounted_segments_[new_segment->id] = new_segment;

    if (on_segment_added_) {
        on_segment_added_(*new_segment);
    }

    return {};
}

tl::expected<void, ErrorCode> P2PSegmentManager::OnUnmountSegment(
    const std::shared_ptr<Segment>& segment) {
    if (on_segment_removed_) {
        on_segment_removed_(*segment);
    }
    return {};
}

tl::expected<size_t, ErrorCode> P2PSegmentManager::UpdateSegmentUsage(
    const UUID& segment_id, size_t usage) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "fail to update segment usage, segment doesn't exist"
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    } else if (!it->second->IsP2PSegment()) {
        LOG(ERROR) << "unexpected segment type"
                   << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    } else if (usage > it->second->size) {
        LOG(ERROR) << "usage is larger than segment size"
                   << ", segment_id=" << segment_id << ", usage=" << usage
                   << ", segment_size=" << it->second->size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    size_t old_usage = it->second->GetP2PExtra().usage;
    it->second->GetP2PExtra().usage = usage;
    return old_usage;
}

size_t P2PSegmentManager::GetSegmentUsage(const UUID& segment_id) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "segment does not exist"
                     << ", segment_id=" << segment_id;
    } else if (!it->second->IsP2PSegment()) {
        LOG(ERROR) << "unexpected segment type"
                   << ", segment_id=" << segment_id;
    } else {
        return it->second->GetP2PExtra().usage;
    }
    return 0;
}

void P2PSegmentManager::ForEachSegment(const SegmentVisitor& visitor) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    for (const auto& [id, segment] : mounted_segments_) {
        if (!segment->IsP2PSegment()) {
            LOG(ERROR) << "P2PSegmentManager only supports P2PSegmentExtraData"
                       << ", segment_id: " << segment->id;
            continue;
        }
        if (visitor(*segment)) {
            break;
        }
    }
}

}  // namespace mooncake
