#include "vsegment/vsegment_transfer.h"

#include <limits>
#include <unordered_map>

namespace mooncake::vsegment {

namespace {

std::string ViewCacheKey(const std::string& partition_id,
                         const std::string& vsegment_id) {
    return partition_id + '\0' + vsegment_id;
}

bool SameImmutableView(const VSegmentView& left,
                       const VSegmentView& right) {
    if (left.vsegment_id != right.vsegment_id ||
        left.partition_id != right.partition_id ||
        left.mapping_algorithm != right.mapping_algorithm ||
        left.stripe_size != right.stripe_size ||
        left.logical_capacity != right.logical_capacity ||
        left.members.size() != right.members.size()) {
        return false;
    }
    for (size_t index = 0; index < left.members.size(); ++index) {
        const auto& a = left.members[index];
        const auto& b = right.members[index];
        if (a.segment_id != b.segment_id || a.base_offset != b.base_offset ||
            a.length != b.length) {
            return false;
        }
    }
    return true;
}

}  // namespace

bool VSegmentViewCache::Find(const std::string& partition_id,
                             const std::string& vsegment_id,
                             VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = views_.find(ViewCacheKey(partition_id, vsegment_id));
    if (found == views_.end()) return false;
    if (view) *view = found->second;
    return true;
}

ErrorCode VSegmentViewCache::Insert(const VSegmentView& view,
                                    std::string* detail) {
    auto validation = ValidateViewStructure(view, detail);
    if (validation != ErrorCode::OK) return validation;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = ViewCacheKey(view.partition_id, view.vsegment_id);
    auto found = views_.find(key);
    if (found != views_.end()) {
        if (!SameImmutableView(found->second, view)) {
            if (detail) {
                *detail = "immutable view identity has another layout";
            }
            return ErrorCode::INVALID_VERSION;
        }
        return ErrorCode::OK;
    }
    while (views_.size() >= max_entries_ && !insertion_order_.empty()) {
        const auto oldest = std::move(insertion_order_.front());
        insertion_order_.pop_front();
        views_.erase(oldest);
    }
    views_.emplace(key, view);
    insertion_order_.push_back(key);
    return ErrorCode::OK;
}

void VSegmentViewCache::Erase(const std::string& partition_id,
                              const std::string& vsegment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = ViewCacheKey(partition_id, vsegment_id);
    views_.erase(key);
    insertion_order_.erase(
        std::remove(insertion_order_.begin(), insertion_order_.end(), key),
        insertion_order_.end());
}

size_t VSegmentViewCache::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return views_.size();
}

VSegmentTransferPlan VSegmentTransferPlanner::Plan(
    const VSegmentDescriptor& replica,
    const std::vector<ClientSlice>& slices) {
    VSegmentTransferPlan plan;
    if (!provider_ || !endpoint_resolver_ || !cache_ ||
        replica.partition_id.empty() || replica.vsegment_id.empty() ||
        replica.length == 0) {
        plan.error = ErrorCode::INVALID_PARAMS;
        plan.detail = "invalid vsegment transfer planner input";
        return plan;
    }
    VSegmentView view;
    if (!cache_->Find(replica.partition_id, replica.vsegment_id, &view)) {
        auto result = provider_->LoadView(replica.partition_id,
                                          replica.vsegment_id, &view,
                                          &plan.detail);
        if (result != ErrorCode::OK) {
            plan.error = result;
            return plan;
        }
        result = cache_->Insert(view, &plan.detail);
        if (result != ErrorCode::OK) {
            plan.error = result;
            return plan;
        }
    }
    if (view.partition_id != replica.partition_id) {
        plan.error = ErrorCode::INVALID_VERSION;
        plan.detail = "cached view belongs to another Partition";
        return plan;
    }
    auto resolved = ResolveTransfer(view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};
    // Resolve each distinct psegment only once per Plan. The sub-request count
    // grows with (length / stripe_size) while the number of distinct segments
    // is bounded by member_count, so with a small stripe this loop used to
    // issue dozens of identical GetPSegmentEndpoint RPCs for the same three
    // answers (e.g. 128 calls for a 512 KiB object with member_count = 3).
    // The cache is local to this call: no lifetime or invalidation concerns.
    std::unordered_map<std::string, PSegmentLocation> endpoints;
    for (auto& request : resolved.requests) {
        auto [entry, inserted] = endpoints.try_emplace(request.segment_id);
        if (inserted) {
            auto result = endpoint_resolver_->ResolveLocation(
                request.segment_id, &entry->second);
            if (result != ErrorCode::OK || entry->second.endpoint.empty()) {
                plan.error = result == ErrorCode::OK
                                 ? ErrorCode::SEGMENT_NOT_FOUND
                                 : result;
                plan.detail =
                    "cannot resolve endpoint for " + request.segment_id;
                plan.requests.clear();
                return plan;
            }
        }
        const PSegmentLocation& location = entry->second;
        if (request.physical_offset >
            std::numeric_limits<uint64_t>::max() - location.base_address) {
            plan.error = ErrorCode::INVALID_PARAMS;
            plan.detail = "psegment target address overflows";
            plan.requests.clear();
            return plan;
        }
        request.physical_offset += location.base_address;
        // NOTE: endpoint is shared by every sub-request of this segment, so it
        // must be copied rather than moved.
        plan.requests.push_back({std::move(request), location.endpoint});
    }
    return plan;
}

VSegmentTransferPlan VSegmentTransferPlanner::PlanWithRoute(
    const VSegmentDescriptor& replica, const std::vector<ClientSlice>& slices,
    const VSegmentRoute& route) {
    VSegmentTransferPlan plan;
    if (replica.partition_id.empty() || replica.vsegment_id.empty() ||
        replica.length == 0) {
        plan.error = ErrorCode::INVALID_PARAMS;
        plan.detail = "invalid vsegment transfer planner input";
        return plan;
    }
    // 1) 身份必须与 descriptor 一致。
    if (route.view.partition_id != replica.partition_id ||
        route.view.vsegment_id != replica.vsegment_id) {
        plan.error = ErrorCode::INVALID_VERSION;
        plan.detail = "inline route identity does not match descriptor";
        return plan;
    }
    // 2) 布局校验：checksum 覆盖 identity / mapping / stripe / 成员。
    if (route.view.checksum != ComputeViewChecksum(route.view)) {
        plan.error = ErrorCode::CHECKSUM_MISMATCH;
        plan.detail = "inline route view checksum mismatch";
        return plan;
    }
    // 3) 成员表必须与 view.members 一一对应（同序、同 segment_id、endpoint 非空）。
    if (route.locations.size() != route.view.members.size()) {
        plan.error = ErrorCode::SEGMENT_NOT_FOUND;
        plan.detail = "inline route member count mismatch";
        return plan;
    }
    for (size_t i = 0; i < route.view.members.size(); ++i) {
        if (route.locations[i].segment_id != route.view.members[i].segment_id) {
            plan.error = ErrorCode::SEGMENT_NOT_FOUND;
            plan.detail = "inline route member order mismatch";
            return plan;
        }
        if (route.locations[i].endpoint.empty()) {
            plan.error = ErrorCode::SEGMENT_NOT_FOUND;
            plan.detail = "inline route endpoint is empty";
            return plan;
        }
    }
    // 4) 逻辑区间 → 物理子请求（纯计算，不发 RPC）。
    auto resolved = ResolveTransfer(route.view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};

    std::unordered_map<std::string, const PSegmentRouteEntry*> locations;
    locations.reserve(route.locations.size());
    for (const auto& location : route.locations)
        locations.emplace(location.segment_id, &location);

    for (auto& request : resolved.requests) {
        auto it = locations.find(request.segment_id);
        if (it == locations.end()) {
            plan.error = ErrorCode::SEGMENT_NOT_FOUND;
            plan.detail = "inline route misses segment " + request.segment_id;
            plan.requests.clear();
            return plan;
        }
        const PSegmentRouteEntry& location = *it->second;
        if (request.physical_offset >
            std::numeric_limits<uint64_t>::max() - location.base_address) {
            plan.error = ErrorCode::INVALID_PARAMS;
            plan.detail = "psegment target address overflows";
            plan.requests.clear();
            return plan;
        }
        request.physical_offset += location.base_address;
        plan.requests.push_back({std::move(request), location.endpoint});
    }
    return plan;
}

}  // namespace mooncake::vsegment
