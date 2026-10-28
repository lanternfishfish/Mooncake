#pragma once

#include <algorithm>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

class VSegmentViewProvider {
   public:
    virtual ~VSegmentViewProvider() = default;
    virtual ErrorCode LoadView(const std::string& partition_id,
                               const std::string& vsegment_id,
                               VSegmentView* view,
                               std::string* detail = nullptr) = 0;
};

struct PSegmentLocation {
    std::string endpoint;
    uint64_t base_address{0};
};
YLT_REFL(PSegmentLocation, endpoint, base_address);

class SegmentEndpointResolver {
   public:
    virtual ~SegmentEndpointResolver() = default;
    virtual ErrorCode ResolveLocation(const std::string& segment_id,
                                      PSegmentLocation* location) = 0;
};

class VSegmentViewCache {
   public:
    explicit VSegmentViewCache(size_t max_entries = 4096)
        : max_entries_(std::max<size_t>(1, max_entries)) {}
    bool Find(const std::string& partition_id, const std::string& vsegment_id,
              VSegmentView* view) const;
    ErrorCode Insert(const VSegmentView& view,
                     std::string* detail = nullptr);
    void Erase(const std::string& partition_id,
               const std::string& vsegment_id);
    size_t Size() const;

   private:
    const size_t max_entries_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, VSegmentView> views_;
    std::deque<std::string> insertion_order_;
};

// Process-wide cache of runtime psegment locations keyed by segment_id.
// A segment_id is a freshly generated UUID per mount, and endpoint/base are
// fixed for the lifetime of that id, so entries never need a TTL. Entries are
// only dropped on capacity pressure or when a transfer failure names their
// endpoint (see Client::ReportFailedEndpointsToMasters).
class SegmentLocationCache {
   public:
    explicit SegmentLocationCache(size_t max_entries = 4096)
        : max_entries_(std::max<size_t>(1, max_entries)) {}
    bool Find(const std::string& segment_id,
              PSegmentLocation* location) const;
    void Insert(const std::string& segment_id,
                const PSegmentLocation& location);
    // Drops every entry served by `endpoint`. One endpoint (host:port) backs
    // all segments mounted from the same holder, so this evicts all of them.
    void EraseByEndpoint(const std::string& endpoint);
    size_t Size() const;

   private:
    const size_t max_entries_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, PSegmentLocation> locations_;
    std::deque<std::string> insertion_order_;
};

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

class VSegmentTransferPlanner {
   public:
    VSegmentTransferPlanner(VSegmentViewProvider* provider,
                            SegmentEndpointResolver* endpoint_resolver,
                            VSegmentViewCache* cache,
                            SegmentLocationCache* location_cache)
        : provider_(provider),
          endpoint_resolver_(endpoint_resolver),
          cache_(cache),
          location_cache_(location_cache) {}

    VSegmentTransferPlan Plan(const VSegmentDescriptor& replica,
                              const std::vector<ClientSlice>& slices);

   private:
    VSegmentViewProvider* provider_;
    SegmentEndpointResolver* endpoint_resolver_;
    VSegmentViewCache* cache_;
    SegmentLocationCache* location_cache_;
};

}  // namespace mooncake::vsegment
