#include "vsegment/vsegment_transfer.h"

// 读路径细分点位（与 client_service.cpp 同一个 SpDiag program）。
#define SPDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#define SPDIAG_PROGRAM_NAME "mooncake_store"
#include "spdiag/auto_perf.h"

#include <limits>

namespace mooncake::vsegment {

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
    // 校验段单独计时（身份 / checksum / 成员顺序）。
    SpDiag::PerfPoint pt_check(PerfKey::VSEGMENT_PLAN_CHECK,
                               SpDiag::PerfLevel::MODULE);
    pt_check.Start();
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
    pt_check.End(0);
    // 展开段单独计时（逻辑区间 → 物理子请求 + 成员查表）。
    SpDiag::PerfPoint pt_expand(PerfKey::VSEGMENT_PLAN_EXPAND,
                                SpDiag::PerfLevel::MODULE);
    pt_expand.Start();
    // 4) 逻辑区间 → 物理子请求（纯计算，不发 RPC）。
    auto resolved = ResolveTransfer(route.view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};

    // 成员数通常 <= 8（route 大小有上限），用线性查表：省掉每读一次的
    // unordered_map 构造 + string 哈希 + 节点分配。
    const auto find_location = [&route](
                                   const std::string& segment_id)
        -> const PSegmentRouteEntry* {
        for (const auto& location : route.locations) {
            if (location.segment_id == segment_id) return &location;
        }
        return nullptr;
    };

    plan.requests.reserve(resolved.requests.size());
    for (auto& request : resolved.requests) {
        const PSegmentRouteEntry* location = find_location(request.segment_id);
        if (location == nullptr) {
            plan.error = ErrorCode::SEGMENT_NOT_FOUND;
            plan.detail = "inline route misses segment " + request.segment_id;
            plan.requests.clear();
            return plan;
        }
        if (request.physical_offset >
            std::numeric_limits<uint64_t>::max() - location->base_address) {
            plan.error = ErrorCode::INVALID_PARAMS;
            plan.detail = "psegment target address overflows";
            plan.requests.clear();
            return plan;
        }
        request.physical_offset += location->base_address;
        plan.requests.push_back({std::move(request), location->endpoint});
    }
    pt_expand.End(0);
    return plan;
}

}  // namespace mooncake::vsegment
