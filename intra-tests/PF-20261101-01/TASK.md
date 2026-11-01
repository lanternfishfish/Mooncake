# 性能测试任务 PF-20261101-01 — 证明 vsegment 的相对优势（只测不改）

> 本文件就在同步分支里，跟着代码走。**任务只做两件事：跑 §4 的三组测试、把 §7 的数据粘贴到 GitHub issue。**
> 不改代码、不提 PR、不往本分支 push、不传二进制或附件。

- **任务 ID**：`PF-20261101-01`（后续任务递增：`PF-<YYYYMMDD>-NN`；issue 标题与正文都要带这个 ID）
- **代码**：当前分支 `sync/pf-20261101-01`，开工先 `git rev-parse HEAD` 并记下（应为 `899db2d1` 或其后继；
  若与下发时告知的 commit 不一致，在 issue 里写明实际值，不要切换分支）
- **结果回传**：**在 GitHub 提 issue**（见 §8），正文纯文本，总量 ≤ 40 KB

---

## 1. 目标（可证伪）

**命题**：在**特定配置**下，开启 vsegment 的读吞吐/尾延迟优于**显式关闭 vsegment**的同一集群。

| 结论 | 判据 |
|---|---|
| vsegment 有优势 | `C1` 的 reader 中位吞吐 ≥ `C3` 的 **1.15×**，且 P99 不劣于 `C3` 的 1.1× |
| 优势来自条带 | 同时 `C1` ≥ `C2` 的 1.10×（`C2` 是单条带对照） |
| 无优势 | 任一不满足 → 如实报数字，**不要调参重试到出好结果** |

**不是**全面评估：只跑 §4 三组，不做参数扫描。

---

## 2. 拓扑与进程（固定）

| 节点 | 角色 |
|---|---|
| node1 | etcd + master + client（**master 采 SpDiag**） |
| node2 | client |
| node3 | client |
| node4 | client + `stress-cluster-bench` writer 与 reader（**bench 采 SpDiag**） |

- 协议：**优先 UB**；不可用则 TCP，并写进 issue。
- 单线程、单 batch：`--num_threads=1 --batch_size=1`，外部 in-flight = 1，`slices=1`。
- 每轮开始前把集群恢复成该组状态（见 §3），**同一轮内不切协议/参数**。

---

## 3. 构建与「显式开/关 vsegment」

### 3.1 构建（SpDiag Layer 1）

```bash
cmake -S . -B build-spdiag -G Ninja -DMOONCAKE_ENABLE_SPDIAG=ON \
  -DSTORE_USE_ETCD=ON -DUSE_ETCD=ON -DUSE_TCP=ON -DUSE_HTTP=ON \
  -DBUILD_UNIT_TESTS=OFF -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build-spdiag --target mooncake_master stress_cluster_bench store -j8
```

构建失败 → issue 里贴最后 30 行错误，停止。

### 3.2 显式关闭 vsegment（C3）

vsegment 的开关 = **etcd 里没有配额快照**，不是命令行 flag：

1. 干净的 etcd 数据目录（新 `--data-dir` 或删除旧目录后重启）；
2. master **不加** `--vsegment_auto_plan`；
3. 校验（两个都必须是 0）：

```bash
curl -s http://node1:<metrics_port>/metrics | grep -E \
  '^mooncake_vsegment_(logical_capacity_bytes|create_success_total) '
```

4. 留证据：`curl -s http://node1:<cvm_port>/segment_view | head -c 400`（贴进 issue 的 `C3-evidence` 块）。

### 3.3 开启 vsegment（C1/C2）

4 个 client 都挂好段之后：

```bash
curl -s -X POST "http://node1:<cvm_port>/vsegment_quota?member_count=4&stripe_size=<§4>&member_extent_size=<§4>"
# 期望 {"status":"ok",...}；返回 VSEGMENT_MOUNT_QUIET_PERIOD 就等 5s 重试，最多 10 次
```

三条都满足才算真开了（把计数贴进 issue）：

```bash
curl -s http://node1:<metrics_port>/metrics | grep -E \
  '^mooncake_vsegment_(logical_capacity_bytes|create_success_total) '   # > 0
grep -c 'vsegment_route_inline entry=GetReplicaList routes=1' <master_log>  # > 0
grep -c 'vsegment_route_inline entry=GetReplicaList routes=0' <master_log>  # 必须 0
```

「开启」失败（capacity 一直 0）→ 别反复试：把 master 日志里 `vsegment`/`quota` 相关 20 行贴进 issue，该组记 `BLOCKED`，继续跑其它组。

---

## 4. 测试矩阵（3 组 × 5 轮 = 15 次）

固定：`value_size=16 MiB`、`keys=64`（≈1 GiB；容量不够降到 32 并记录）、单线程、单 batch。

| 组 | vsegment | stripe_size | member_extent_size | 目的 |
|---|---|---|---|---|
| **C1** | ON | 4 MiB | 16 MiB | 4 成员条带（16 MiB / 4 MiB = 4 条）→ 期望最高聚合带宽 |
| **C2** | ON | 16 MiB | 16 MiB | 单条带对照（若 C1≈C2，说明优势不是条带带来的） |
| **C3** | OFF（§3.2） | — | — | 基线 |

每轮：

```bash
# writer 预填（node4，输出不必回传）
stress_cluster_bench --master_server=node1:<rpc> --metadata_server=P2PHANDSHAKE \
  --protocol=<ub|tcp> --scenario=remote_memory --role=writer \
  --global_segment_size=0 --value_size=16777216 --num_keys=64 \
  --num_threads=1 --batch_size=1

# reader（node4）—— 这就是 §7 的数据源
stress_cluster_bench --master_server=node1:<rpc> --metadata_server=P2PHANDSHAKE \
  --protocol=<ub|tcp> --scenario=remote_memory --role=reader \
  --global_segment_size=0 --value_size=16777216 --num_keys=64 \
  --num_threads=1 --batch_size=1
```

每组重复 5 轮（`r1..r5`）；**每轮都重新确认 §3.2/§3.3 的校验条件**（尤其 C3，别让上一组配额残留）。

---

## 5. 采集点

| # | 采什么 | 在哪 |
|---|---|---|
| 1 | reader 的 **summary**（原样表格） | node4，reader 输出末尾 |
| 2 | **SpDiag matrix**（阶段 × 耗时） | node1（master）、node4（bench） |
| 3 | 火焰图分析（可选） | node4，只交结论 |

附带计数（很便宜，顺手记）：`vsegment_create_success_total`、`vsegment_stale_route_total`、
客户端 `mooncake_client_rpc_count{rpc_name="GetReplicaList"|"PutStart"|"PutEnd"}`（核对 GET=1 / PUT=2 / 旧 RPC=0）。

---

## 6. 每轮记录（贴进 issue，一行一轮）

```text
run=C1-r1 ts=2026-11-01T__:__ node4 protocol=ub vsegment=ON stripe=4MiB extent=16MiB object=16MiB keys=64 threads=1 batch=1 capacity=<...> create=<...> routes1=<...> routes0=<...> tput_MBps=<...> avg_us=<...> p50_us=<...> p99_us=<...> p999_us=<...>
```

---

## 7. 数据规格（贴 issue 的文本，不用附件）

### 7.1 `reader-summary`（≤ 12 KB）

每个 run 一段，**原样保留**（从 `Total queries:` 到 `P999:`）：

```text
=== C1-r1 ===
  Total queries:    64 (failed: 0)
  Throughput:       1234.56 MB/s
    Avg:        12345.67
    P50:        12000.00
    P99:        18000.00
    P999:       19000.00
```

### 7.2 `spdiag-master` / `spdiag-bench`（各 ≤ 14 KB）

每个 run 只留 **top 20 行**（按 P99 降序）：

```text
=== C1-r1 master ===
PerfKey                            P50_us  P99_us  count
MASTER_RPC_GET_REPLICA_LIST            120     350     64
GET_REPLICA_BUILD                       40      90     64
```

关心的 PerfKey：`MASTER_RPC_*`、`GET_SINGLE_*`、`PUT_*`、`VSEGMENT_*`。
现场 SpDiag CLI 输出格式不同没关系，**保留 PerfKey/P50/P99/count 四列**即可；不要贴二进制/原始 dump。

### 7.3 火焰图结论（可选，≤ 3 KB）

```text
=== C1-r1 reader flame top5 ===
mooncake::TcpTransport::worker   18.2%
mooncake::Client::TransferData    9.4%
```

### 7.4 元信息（≤ 1 KB）

```text
task=PF-20261101-01
commit=<git rev-parse HEAD>
protocol=ub
groups: C1(vsegment=ON,stripe=4MiB,extent=16MiB) C2(vsegment=ON,stripe=16MiB,extent=16MiB) C3(vsegment=OFF)
object=16777216 keys=64 threads=1 batch=1 repeats=5
blocked=<如有>
notes=<任何偏差：容量、节点、重试等>
```

---

## 8. 结果回传：提一个 issue

1. 仓库：`lanternfishfish/Mooncake`（没有 issue 权限就用用户指定的临时仓库）。
2. 标题：`[PF-20261101-01] vsegment vs non-vsegment 测试结果`
3. 正文按下面顺序（**纯文本 + code fence**，不要附件）：

```markdown
### PF-20261101-01 · 元信息
（§7.4 的内容）

### reader-summary
（§7.1）

### spdiag-master
（§7.2）

### spdiag-bench
（§7.2）

### C3-evidence
（§3.2 的 segment_view 片段 + 两个 0 计数）

### flame（可选）
（§7.3）

### 结论
C1/C3 吞吐比 = __；P99 比 = __；是否满足 §1 判据：是/否；一句话原因。
```

4. 正文超过 40 KB → **拆成多条评论**（第 1 条放元信息+结论，后续评论放数据），标题保持一致并标注 `(2/3)`。
5. 完成后在 issue 里回一句「PF-20261101-01 done」，便于蓝区轮询。

---

## 9. 自检（提 issue 前逐条确认）

- [ ] issue 标题/正文都含 `PF-20261101-01`，元信息里有 `commit=`、`protocol=`、`task=`
- [ ] 15 个 run 都有 §6 的一行记录（或 `blocked` 里写清原因）
- [ ] C3：`capacity=0` 且 `create=0`，并有 `C3-evidence` 片段
- [ ] C1/C2：`capacity>0`、`routes1>0`、`routes0=0`
- [ ] `reader-summary` 是原样表格（不是只有平均值）
- [ ] `spdiag-master` 与 `spdiag-bench` 都非空（为空就在 `blocked` 说明）
- [ ] 没有附件、没有二进制、没有完整日志

---

## 10. 禁止

1. 不改代码、不 commit、不 push、不提 PR（本分支对你只读）。
2. 不为「好看的数字」改参数重跑；不好看也如实交。
3. 不在同一轮混协议。
4. 任何与 §2/§4 的偏差都要写进 `notes`。
