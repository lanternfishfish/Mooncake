#include "vsegment/vsegment_transfer.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <limits>
#include <string>
#include <unordered_map>

namespace mooncake::vsegment {

namespace {

// Shadow mode: always resolve through the legacy view cache + endpoint RPC,
// but compare the result against the inline table. Used by the rollback drill
// to prove the inline data matches the old path before switching reads.
bool VSegmentLayoutShadowEnabled() {
    // Re-read per call (not cached) so the rollback drill and unit tests can
    // toggle it on a running process.
    const char* env = std::getenv("MC_VSEGMENT_LAYOUT_SHADOW");
    if (env == nullptr) return false;
    const std::string value(env);
    return !(value == "0" || value == "false" || value == "no" ||
             value == "off");
}

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
    VSegmentView candidate;
    bool have_layout = false;
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
            candidate.vsegment_id = it->vsegment_id;
            candidate.partition_id = it->partition_id;
            candidate.mapping_algorithm =
                static_cast<MappingAlgorithm>(it->mapping_algorithm);
            candidate.stripe_size = it->stripe_size;
            candidate.logical_capacity = it->logical_capacity;
            candidate.members.reserve(it->members.size());
            bool endpoints_ok = true;
            for (const auto& member : it->members) {
                candidate.members.push_back(
                    PSegmentExtent{member.segment_id, member.base_offset,
                                   member.length});
                if (member.endpoint.empty()) {
                    // Unmounted segment (e.g. standby recovery): the entry is
                    // unusable, fall back to the legacy resolution path.
                    endpoints_ok = false;
                    break;
                }
                layout_locations[member.segment_id] =
                    PSegmentLocation{member.endpoint, member.base_address};
            }
            candidate.checksum = it->checksum;
            std::string validation_detail;
            if (endpoints_ok &&
                ValidateViewStructure(candidate, &validation_detail) ==
                    ErrorCode::OK) {
                view = std::move(candidate);
                have_layout = true;
                use_layout = !VSegmentLayoutShadowEnabled();
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
    if (have_layout && !use_layout) {
        // Shadow comparison: layout entry vs legacy resolution.
        static std::atomic<uint64_t> view_mismatches{0};
        if (!SameImmutableView(candidate, view)) {
            const uint64_t count = ++view_mismatches;
            if (count <= 5 || count % 1000 == 0) {
                LOG(WARNING) << "vsegment layout shadow view mismatch"
                             << " vsegment=" << replica.vsegment_id
                             << " total_mismatches=" << count;
            }
        }
        static std::atomic<uint64_t> mismatches{0};
        for (const auto& [segment_id, layout_location] : layout_locations) {
            auto legacy = endpoints.find(segment_id);
            if (legacy == endpoints.end()) continue;
            if (legacy->second.endpoint == layout_location.endpoint &&
                legacy->second.base_address ==
                    layout_location.base_address) {
                continue;
            }
            const uint64_t count = ++mismatches;
            if (count <= 5 || count % 1000 == 0) {
                LOG(WARNING) << "vsegment layout shadow mismatch"
                             << " segment=" << segment_id
                             << " legacy_endpoint=" << legacy->second.endpoint
                             << " inline_endpoint=" << layout_location.endpoint
                             << " legacy_base=" << legacy->second.base_address
                             << " inline_base=" << layout_location.base_address
                             << " total_mismatches=" << count;
            }
        }
    }
    return plan;
}

}  // namespace mooncake::vsegment
