#include "vsegment/vsegment_route.h"

#include <unordered_set>

#include "mooncake_logging.h"

namespace mooncake::vsegment {

tl::expected<std::vector<VSegmentRoute>, ErrorCode> BuildVSegmentRoutes(
    const std::vector<Replica::Descriptor>& replicas,
    const LoadViewFn& load_view, const ResolveEndpointFn& resolve_endpoint) {
    std::vector<VSegmentRoute> routes;
    if (replicas.empty()) return routes;
    if (!load_view || !resolve_endpoint)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    // 去重键：partition_id + '\x1f' + vsegment_id（两者都不可能含 \x1f）。
    std::unordered_set<std::string> seen;
    for (const auto& replica : replicas) {
        if (!replica.is_vsegment_replica()) continue;
        const auto& descriptor = replica.get_vsegment_descriptor();
        std::string key = descriptor.partition_id;
        key.push_back('\x1f');
        key.append(descriptor.vsegment_id);
        if (!seen.insert(key).second) continue;

        VSegmentView view;
        const ErrorCode load =
            load_view(descriptor.partition_id, descriptor.vsegment_id, &view);
        if (load != ErrorCode::OK) {
            LOG(WARNING) << "build vsegment route failed: partition="
                         << descriptor.partition_id
                         << ", vsegment=" << descriptor.vsegment_id
                         << ", error=" << toString(load);
            return tl::make_unexpected(load);
        }
        if (view.partition_id != descriptor.partition_id ||
            view.vsegment_id != descriptor.vsegment_id) {
            LOG(WARNING) << "build vsegment route failed: view identity mismatch"
                         << ", partition=" << descriptor.partition_id
                         << ", vsegment=" << descriptor.vsegment_id
                         << ", view_partition=" << view.partition_id
                         << ", view_vsegment=" << view.vsegment_id;
            return tl::make_unexpected(ErrorCode::INVALID_VERSION);
        }

        VSegmentRoute route;
        route.view = std::move(view);
        route.locations.reserve(route.view.members.size());
        for (const auto& member : route.view.members) {
            std::string endpoint;
            uint64_t base_address = 0;
            if (!resolve_endpoint(member.segment_id, &endpoint, &base_address) ||
                endpoint.empty()) {
                LOG(WARNING) << "build vsegment route failed: psegment "
                             << member.segment_id << " unavailable, partition="
                             << route.view.partition_id
                             << ", vsegment=" << route.view.vsegment_id;
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            route.locations.push_back(
                PSegmentRouteEntry{member.segment_id, std::move(endpoint),
                                   base_address});
        }
        routes.push_back(std::move(route));
    }
    return routes;
}

const VSegmentRoute* FindRoute(
    const std::vector<VSegmentRoute>* routes,
    const VSegmentDescriptor& descriptor) {
    if (routes == nullptr) return nullptr;
    for (const auto& candidate : *routes) {
        if (candidate.view.partition_id == descriptor.partition_id &&
            candidate.view.vsegment_id == descriptor.vsegment_id) {
            return &candidate;
        }
    }
    return nullptr;
}

void LogRouteStats(const char* entry,
                   const std::vector<VSegmentRoute>& routes) {
    if (!mooncake::logging::ShouldSampleHiFreqLog()) return;
    size_t members = 0;
    size_t est_bytes = 0;
    for (const auto& route : routes) {
        members += route.locations.size();
        // 估算序列化体积：view 的字符串 + 每成员 segment_id/endpoint + 定长字段。
        est_bytes += route.view.partition_id.size() +
                     route.view.vsegment_id.size() + 64;
        for (const auto& location : route.locations)
            est_bytes += location.segment_id.size() + location.endpoint.size() +
                         16;
    }
    MC_LOG(INFO) << "vsegment_route_inline entry=" << entry
                 << " routes=" << routes.size() << " members=" << members
                 << " est_bytes=" << est_bytes;
}

}  // namespace mooncake::vsegment
