# cs223_txn

工程化分层事务框架（Storage / Txn+CC / Workload / Bench），支持 workload1 与 workload2 的逻辑隔离，并支持可插拔并发控制策略。

## Build

```bash
mkdir -p build
cd build
cmake ..
cmake --build . -j
```

## Run (NoCC)

### workload1

```bash
./cs223_txn \
  --input ../data/workload1/input1.txt \
  --workload ../data/workload1/workload1.txt \
  --workload_name w1 \
  --storage inmem \
  --cc no_cc \
  --threads 8 --duration 5 --p_hot 0.8 --hotset_size 20
```

### workload2

```bash
./cs223_txn \
  --input ../data/workload2/input2.txt \
  --workload ../data/workload2/workload2.txt \
  --workload_name w2 \
  --storage inmem \
  --cc no_cc \
  --threads 8 --duration 5 --p_hot 0.8 --hotset_size 50
```

## CC 插件

- `no_cc`: 已实现
- `occ`: 预留接口（当前为占位 stub）
- `c2pl`: 预留接口（当前为占位 stub）

后续实现 OCC/C2PL 时，仅需替换 `src/txn/cc/*.cpp`，无需改 workload 与 storage。
