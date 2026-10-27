#include "vsegment/vsegment_transfer.h"

#include <algorithm>
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
    return Plan(replica, slices, nullptr);
}

VSegmentTransferPlan VSegmentTransferPlanner::Plan(
    const VSegmentDescriptor& replica,
    const std::vector<ClientSlice>& slices,
    const std::vector<VSegmentLayoutEntry>* layouts) {
    VSegmentTransferPlan plan;
    if (!provider_ || !endpoint_resolver_ || !cache_ ||
        replica.partition_id.empty() || replica.vsegment_id.empty() ||
        replica.length == 0) {
        plan.error = ErrorCode::INVALID_PARAMS;
        plan.detail = "invalid vsegment transfer planner input";
        return plan;
    }
    VSegmentView view;
    bool use_layout = false;
    std::unordered_map<std::string, PSegmentLocation> layout_locations;
    if (layouts != nullptr) {
        auto it = std::find_if(
            layouts->begin(), layouts->end(),
            [&](const VSegmentLayoutEntry& entry) {
                return entry.vsegment_id == replica.vsegment_id;
            });
        if (it != layouts->end() &&
            it->partition_id == replica.partition_id) {
            VSegmentView candidate;
            candidate.vsegment_id = it->vsegment_id;
            candidate.partition_id = it->partition_id;
            candidate.mapping_algorithm =
                static_cast<MappingAlgorithm>(it->mapping_algorithm);
            candidate.stripe_size = it->stripe_size;
            candidate.logical_capacity = it->logical_capacity;
            candidate.members.reserve(it->members.size());
            for (const auto& member : it->members) {
                candidate.members.push_back(
                    PSegmentExtent{member.segment_id, member.base_offset,
                                   member.length});
                layout_locations[member.segment_id] =
                    PSegmentLocation{member.endpoint, member.base_address};
            }
            candidate.checksum = it->checksum;
            std::string validation_detail;
            if (ValidateViewStructure(candidate, &validation_detail) ==
                ErrorCode::OK) {
                view = std::move(candidate);
                use_layout = true;
            } else {
                layout_locations.clear();
            }
        }
    }
    if (!use_layout) {
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
    }
    auto resolved = ResolveTransfer(view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};
    // Resolve each distinct psegment only once per Plan. The sub-request count
    // grows with (length / stripe_size) while the number of distinct segments
    // is bounded by member_count, so with a small stripe this loop used to
    // issue dozens of identical GetPSegmentEndpoint RPCs for the same three
    // answers (e.g. 128 calls for a 512 KiB object with member_count = 3).
    // When the response carried an inline layout, endpoints come from that
    // table instead of the resolver RPC.
    std::unordered_map<std::string, PSegmentLocation> endpoints;
    for (auto& request : resolved.requests) {
        auto [entry, inserted] = endpoints.try_emplace(request.segment_id);
        if (inserted) {
            if (use_layout) {
                auto found = layout_locations.find(request.segment_id);
                if (found == layout_locations.end()) {
                    plan.error = ErrorCode::SEGMENT_NOT_FOUND;
                    plan.detail =
                        "layout missing endpoint for " + request.segment_id;
                    plan.requests.clear();
                    return plan;
                }
                entry->second = found->second;
            } else {
                auto result = endpoint_resolver_->ResolveLocation(
                    request.segment_id, &entry->second);
                if (result != ErrorCode::OK ||
                    entry->second.endpoint.empty()) {
                    plan.error = result == ErrorCode::OK
                                     ? ErrorCode::SEGMENT_NOT_FOUND
                                     : result;
                    plan.detail =
                        "cannot resolve endpoint for " + request.segment_id;
                    plan.requests.clear();
                    return plan;
                }
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

}  // namespace mooncake::vsegment
