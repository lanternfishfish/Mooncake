#pragma once

#include <string>
#include <vector>

#include "replica.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

// 物理段的运行时位置（master 侧 SegmentRegistry 查表结果）。
struct PSegmentLocation {
    std::string endpoint;
    uint64_t base_address{0};
};
YLT_REFL(PSegmentLocation, endpoint, base_address);

struct EndpointTransferSubRequest {
    TransferSubRequest transfer;
    std::string endpoint;
};

struct VSegmentTransferPlan {
    ErrorCode error{ErrorCode::OK};
    std::vector<EndpointTransferSubRequest> requests;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};

// 只消费入口 RPC 内联的 route：不发任何控制面 RPC，也没有 view/endpoint 缓存。
class VSegmentTransferPlanner {
   public:
    VSegmentTransferPlanner() = default;

    VSegmentTransferPlan PlanWithRoute(const VSegmentDescriptor& replica,
                                       const std::vector<ClientSlice>& slices,
                                       const VSegmentRoute& route);
};

}  // namespace mooncake::vsegment
