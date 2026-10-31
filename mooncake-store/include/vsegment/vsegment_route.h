#pragma once

// vsegment 路由内联（L1）：把「视图 + 每个成员的运行时位置」打包成 VSegmentRoute，
// 由入口 RPC 响应携带给客户端，从而消除旧的 view/endpoint 解析 RPC。
//
// 上游友好：本文件与 vsegment_route.cpp 是 fork 独有文件，所有去重 / 完整性校验
// 逻辑都在这里；master_service 只提供两个薄回调，避免在上游文件里堆积逻辑。

#include <functional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "replica.h"
#include "types.h"
#include "vsegment/vsegment.h"

namespace mooncake::vsegment {

// 加载某 (partition, vsegment) 的视图快照。
using LoadViewFn = std::function<ErrorCode(
    const std::string& partition_id, const std::string& vsegment_id,
    VSegmentView* view)>;

// 解析某物理段的运行时位置（与 master 侧唯一查表语义保持一致）。
using ResolveEndpointFn = std::function<bool(
    const std::string& segment_id, std::string* endpoint,
    uint64_t* base_address)>;

// 为一批副本构建要随响应下发的 route：
//   - 只处理 vsegment 副本，非 vsegment 副本忽略；
//   - 按 (partition_id, vsegment_id) 去重，同一 vsegment 只序列化一份；
//   - 任一必需成员缺失 / endpoint 为空 → 返回错误，**绝不返回部分 route**。
tl::expected<std::vector<VSegmentRoute>, ErrorCode> BuildVSegmentRoutes(
    const std::vector<Replica::Descriptor>& replicas,
    const LoadViewFn& load_view, const ResolveEndpointFn& resolve_endpoint);

}  // namespace mooncake::vsegment
