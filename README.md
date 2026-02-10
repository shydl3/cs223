# cs223_txn

工程化分层事务框架（Storage / Txn+CC / Workload / Bench），支持 workload1 与 workload2 的逻辑隔离，并支持可插拔并发控制策略。

## Build

```bash
mkdir -p build
cd build
cmake ..
cmake --build . -j
```

## Run (OCC / C2PL)

### workload1

```bash
./cs223_txn \
  --input ../data/workload1/input1.txt \
  --workload ../data/workload1/workload1.txt \
  --workload_name w1 \
  --storage inmem \
  --cc occ \
  --threads 8 --duration 5 --p_hot 0.8 --hotset_size 20
```

### workload2

```bash
./cs223_txn \
  --input ../data/workload2/input2.txt \
  --workload ../data/workload2/workload2.txt \
  --workload_name w2 \
  --storage inmem \
  --cc c2pl \
  --threads 8 --duration 5 --p_hot 0.8 --hotset_size 50
```

## CC 插件状态

- `no_cc`: baseline
- `occ`: 已实现（顺序验证 + 私有写集提交）
- `c2pl`: 已实现（Conservative 2PL，执行前预申请全锁）

## 指标输出

控制台与 CSV 包含：
- committed / aborted / retries
- abort_rate / retry_per_commit
- lock_conflicts / validation_conflicts
- throughput_tps
- avg_commit_latency_ms
- avg_response_latency_ms + p50/p95/p99 response latency
- per-template response latency 分布统计

## 一键实验（生成作图数据）

```bash
./scripts/run_experiments.sh
```

输出目录：`build/exp/`
