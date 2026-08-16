# AI Inference Storage Showcase

## Cache-Aware LLM Router & KV Storage Protocols

> 一个经过精选、可在普通 CPU 环境复现的系统展示仓库，聚焦缓存感知的 LLM 请求路由、KV 存储正确性，以及有证据边界的实验结论。

## 项目定位

本仓库研究两个收敛的问题：实现 OpenAI-compatible completion/chat HTTP 子集的 Router 如何使用近似前缀缓存元数据，同时保持 Worker 生命周期安全；独立的多层 KV 协议如何在主机内存、本地文件和 S3-compatible 接口之间维护完整身份与删除正确性。

真实运行主线是 Go HTTP Router 与 streaming proxy。KVStore 是独立的 Python 协议原型，没有进入模型引擎的 KV 热路径。一个精简的 C++/Python I/O 模块通过 Schema 校验的文件契约向 KV 成本模型提供 tier profile。默认构建不需要 Docker、GPU、集群、对象服务或云账号。

## 系统形态

```text
图例
  ─────▶  真实运行调用
  ═════▶  有版本的文件契约
  - - -   相互独立；不存在运行时 payload 集成

Client ──completion/chat HTTP 子集──▶ Go Router ──HTTP / streamed body──▶ Worker
                                      │
                                      ├─────▶ approximate block identity
                                      ├─────▶ CacheIndex metadata
                                      └─────▶ routing + 锁内 revalidate/inflight

Local I/O Profile ══ tier_profile.json + Schema ══▶ KVStore cost model

Go Router - - - Python KVStore protocol
                   ├─────▶ host-memory tier
                   ├─────▶ file-backed tier
                   └─────▶ S3-compatible abstraction
```

虚线不是调用箭头：Router 的缓存元数据与 KVStore 的 payload 字节具有不同的身份和生命周期。本地 Demo 使用两个明确标注的 fake Worker，不把它们描述成 vLLM。

## 已实现内容

- **Go Router：** completion/chat 子集解析与代理、确定性的近似 tokenization、namespace-isolated chained block hash、CacheIndex TTL/event/sequence、可路由 Worker 过滤、多种路由策略、pick 后锁内 revalidate、Router-owned inflight 生命周期计数、drain/remove 检查、streaming proxy，以及受限且脱敏的观测信息。
- **Python KVStore 协议：** 完整 namespace key、memory/file-backed tier、可选 S3-compatible client 边界、payload-first/metadata-last、身份/长度/checksum 复验、原子的 lookup-and-acquire/release、单进程 owner lock 和 epoch 清理、deleting tombstone、segment append/compaction、prefetch/eviction，以及 load/prefetch/recompute 决策。
- **Local I/O Profile：** C++17 buffered、`pread`、`mmap`、vectored、`O_DIRECT` 路径，最小 matrix runner、版本化 JSON Schema、profile generator 和经过测试的 KV importer。

## 快速开始

需要 Go 1.22+、Python 3.10+、CMake 3.16+、C++17 编译器，以及 `requirements-ci.txt` 中的一项 Python 依赖。

```bash
python3 -m pip install -r requirements-ci.txt
make test
make demo
make demo-kv
make audit
make clean
```

测试和 Demo 不访问外部 endpoint。`make demo` 只使用回环端口 `18080`、`18081`、`18082`、`19090`；端口被占用时会明确失败。

## 设计要点

- block identity 按 parent hash 链接各块，并包含 tenant/model/tokenizer/adapter/modality/cache-salt namespace；它只对本仓库的近似算法稳定，不是模型引擎权威身份。
- CacheIndex 过滤过期 observation，对非空 event ID 去重，并要求 `ApplyEvent` 的正 sequence 按 Worker 递增；sequence zero 是显式的无序本地模式。
- pick 只是候选结果；Router 必须重新持有 Worker 锁、复验 routability，并增加 Router-owned inflight 生命周期计数。该计数不会保留 Worker 算力、内存或 KV 容量；上游或 stream 失败时也不会改选其他 Worker 重试。
- file-backed 与 S3-compatible store 都先发布 payload，再提交 metadata；load 会复验完整 key、长度与 checksum。
- tombstone 先使删除在逻辑上可见，再清理物理数据；append-only segment 只在调用者显式执行同步 compaction 时回收。
- cost model 明确区分 `load`、异步 `prefetch` 与 `recompute`。

完整理由见 [docs/design-decisions.md](docs/design-decisions.md)。

## 本地 Demo

`make demo` 会把 Router 构建到临时目录，启动两个本地 fake Worker，并验证：

1. 两次 round-robin 请求，其中一次为 streamed chat response；
2. 向 CacheIndex 注入一个受控的近似 block 事件；
3. `prefix_hash` 选择具有匹配缓存元数据的 Worker。

每次决策都会输出 selected Worker、strategy 和脱敏 request hash。所有服务和临时文件均在 `finally` 路径中清理。fake Worker 只是 HTTP test double。

`make demo-kv` 独立展示 store → load → corruption detection → tombstone-backed delete。

## 测试

| 命令 | 范围 |
|---|---|
| `make test-router` | format、build、vet、单元/契约测试、race 测试 |
| `make test-kv` | 协议、tier、owner epoch、tombstone、corruption、compaction |
| `make test-io` | C++ build、五引擎 smoke test、profile contract |
| `make test-demo` | 回环端口冲突与立即重跑 regression test |
| `make audit` | Markdown link、隐私/secret、tier-profile Schema、License 状态 |
| `make test` | 三组 CPU 测试 |

CI 执行同一组入口、Router Demo、清理和最终 Git worktree drift 检查。

## 证据

当前实现声明以本地测试为主。少量历史硬件/集群观察只以受限功能摘要保留在 [docs/evidence-and-limitations.md](docs/evidence-and-limitations.md)。私有云 raw artifacts 不在本仓库中，这些摘要不能整体证明当前 HEAD。

仓库中的 tier-profile fixture 是 synthetic contract data，不是 benchmark 证据。复现边界见 [docs/reproduction.md](docs/reproduction.md)。

## 限制

- 本仓库不是 production-grade 端到端 LLM Serving 平台。
- OpenAI 兼容性只覆盖已实现的 `POST /v1/completions` 和 `POST /v1/chat/completions` 请求/响应代理子集，不代表完整 API conformance。
- approximate Router block identity 不等于 vLLM 权威 KV identity。
- Cache overlap 只是对独立、未过期 chained block ID observation 的计数，不能证明存在连续可复用前缀或有效 payload。
- KVStore 没有接入真实 vLLM/LMCache KV payload 热路径。
- 没有 production telemetry、automatic Worker discovery、Router HA 或透明 automatic failover。
- Router-owned inflight 只是单 Router 的生命周期计数，不是 capacity reservation。上游尝试开始后——尤其是已写出 stream 字节后——Router 不会换到其他 Worker 重试。
- 没有实现 RDMA、NIXL、GDS、CXL 或 SPDK 数据面。
- `MemoryTier` 是 host memory，不是 HBM；历史类名 `NVMeTier` 包装的是 file-backed abstraction，不是经过验证的生产 NVMe engine。
- file-backed store 默认不提供突然掉电后的强 durable commit 保证。
- SQLite owner lock 是单进程协调，不是 distributed lease。
- Segment compaction 是显式的同步维护调用，不是后台在线 compactor。Checksum mismatch 会被显式返回并使坏 location 失效，不会被自动转换为 recompute。
- Tier profile 通过经校验的文件进入模型，不是 live telemetry。
- 历史 LMCache 工作只证明功能路径成立，没有稳定或普遍加速证据。
- 历史实验摘要不能证明当前 revision 的完整行为，部分摘要缺少 source revision。

本仓库采用 [MIT License](LICENSE) 开源。

## 开发方式

本仓库是较大私有实验工作区的精选公开版。AI 工具参与了大量初始实现和文档整理；维护者负责问题定义、高层架构、关键不变量、实验设计、审查标准和结论边界，并对本公开版保留的核心组件进行了重新审查与验证。

该披露不替代代码审查和测试证据。职责与验证方式见 [docs/development-approach.md](docs/development-approach.md)。

## 仓库地图

- [`router/`](router/)：Go Router runtime、配置和测试。
- [`kvstore/`](kvstore/)：独立 Python KV storage protocol 与测试。
- [`io-profile/`](io-profile/)：精简本地 I/O 与 profile 支撑。
- [`shared/`](shared/)：跨语言 vectors、Schema 与 contract fixture。
- [`examples/local-demo/`](examples/local-demo/)：hermetic Router/KV Demo。
- [`docs/`](docs/)：架构、设计决策、证据边界和复现说明。
- [`scripts/`](scripts/)：本地审计与清理门。

## 审查路径

建议按以下顺序阅读：

1. [Router request path](router/internal/proxy/handler.go) 和 [routing strategies](router/internal/routing/strategies.go)。
2. [Approximate tokenization](router/internal/blockhash/tokenization.go)、[chained identity](router/internal/blockhash/blockhash.go)、[CacheIndex](router/internal/cacheindex/index.go)。
3. [Metadata owner/tombstone protocol](kvstore/kvstore/metadata_store.py)、[file-backed tier](kvstore/kvstore/nvme_tier.py)、[tier manager](kvstore/kvstore/tier_manager.py)。
4. [Tier-profile Schema](shared/schema/tier_profile.schema.json)、[generator](io-profile/python/io_path_bench/tier_profile.py)、[importer](kvstore/kvstore/tier_profile_import.py)。
5. 在引用任何实验结论前先阅读 [证据边界](docs/evidence-and-limitations.md)。
