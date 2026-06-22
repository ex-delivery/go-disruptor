# go-disruptor

[English](README.md) | **简体中文**

一个面向超低延迟生产者/消费者流水线的泛型、无锁环形缓冲区，仿照
[LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) 设计。

热路径零分配：槽位是预先分配、随序号推进而复用的 `T` 值。协调通过按缓存行
隔离的原子游标完成，不使用 channel 或互斥锁。

```
goos: darwin  goarch: arm64  cpu: Apple M4  (Go 1.26.2)

# 吞吐 — 0 allocs/op
BenchmarkSPSC            9.4 ns/op    # disruptor：1 生产者 -> 1 消费者
BenchmarkMPSC          282   ns/op    # disruptor：4 生产者 -> 1 消费者
BenchmarkChannelSPSC    25   ns/op    # buffered channel，对照

# 端到端延迟（发布 -> 消费）
BenchmarkLatencySPSC          p50 ~0     p99 ~5µs     # ~0 = 快过时钟分辨率
BenchmarkChannelLatencySPSC   p50 ~67µs  p99 ~152µs   # buffered channel
```

## 特性

- **泛型** —— `Disruptor[T]` 支持任意值类型；无 `interface{}`、无装箱。
- **热路径零分配** —— 槽位一次性预建，发布/消费复用同一批对象。
- **单生产者与多生产者** —— 可选极速的单写者路径，或基于 CAS 的多写者定序器。
- **阻塞 / 非阻塞 / 可取消认领** —— `Next` 施加背压；`TryNext` 满环立即失败；
  `NextContext` 阻塞但可被 ctx 取消。
- **DAG 消费者图** —— 消费者可依赖其他消费者，构建并行阶段与扇出/扇入拓扑。
- **work-pool 负载均衡** —— N 个 worker 竞争消费同一流，每事件仅处理一次（区别于广播式消费者）。
- **可插拔等待策略** —— 在延迟与 CPU 之间权衡：忙等、让出（默认）、休眠。
- **有界优雅关闭** —— `Stop` 先排空全部已发布事件；`StopContext` 带超时，卡死的消费者也无法挂起关闭。
- **panic 隔离** —— 逐消费者/逐池处理器，或全局 `WithPanicHandler` 默认，恢复 panic 批次并继续。
- **可观测性** —— 拉取式 `Stats`（lag、占用、背压计数）加可选 `WithMetrics` 采样，均不碰热路径。
- **防伪共享** —— 每个热点游标都按缓存行填充（amd64/arm64 上 128 B，其余 64 B）。

## 安装

```sh
go get github.com/ex-delivery/go-disruptor
```

要求 Go 1.26.2 或更高版本。

## 快速开始（单生产者，单消费者）

```go
type Event struct{ Value int64 }

d := disruptor.NewDisruptor(1024, func() Event { return Event{} })

c := d.Consumer(func(buf []Event, mask, lo, hi int64) {
    for s := lo; s <= hi; s++ {
        _ = buf[s&mask] // 处理槽位
    }
})
d.RegisterConsumer(c)
d.Start()
defer d.Stop()

seq := d.Next(1)        // 认领一个槽位（背压下会阻塞）
d.Get(seq).Value = 42   // 就地填充
d.Publish(seq, seq)     // 对消费者可见
```

`NewDisruptor` 会把容量向上取整到 2 的幂，从而用 `seq & mask` 代替取模来计算
槽位下标。

## 多生产者

传入 `WithProducerFunc(NewMultiProducer)` 即可允许多个写者 goroutine 并发写入。
默认的 `NewSingleProducer` 更快，但只对恰好一个写者安全。

```go
d := disruptor.NewDisruptor(1024, newEvent,
    disruptor.WithProducerFunc(disruptor.NewMultiProducer))
// 现在任意数量的 goroutine 都可以调用 d.Next / d.Publish。
```

## 非阻塞发布

`Next` 会（按等待策略）阻塞，直到最慢的消费者腾出空间；`TryNext` 则在满环时
立即返回 `(0, false)` —— 这是在「必须同时能响应关闭」的 goroutine 里发布的安全
方式，因为 `Stop` 无法唤醒阻塞在 `Next` 里的生产者。

```go
if seq, ok := d.TryNext(1); ok {
    d.Get(seq).Value = 42
    d.Publish(seq, seq)
} else {
    // 此刻环已满 —— 丢弃、重试或退避
}

free := d.RemainingCapacity() // 当前空闲槽位数（并发下为估算值）
```

## 依赖图（DAG）

一个消费者可以依赖其他消费者，从而只在它们处理过某序号之后才处理该序号。由此
可构建「先扇出再扇入」的菱形拓扑：

```go
b := d.Consumer(handlerB)
c := d.Consumer(handlerC)
merge := d.Consumer(handlerMerge).Depends(b, c) // 仅在 b 和 c 之后运行
d.RegisterConsumer(b, c, merge)
```

所有 `Depends` 连线必须在 `Start` 之前完成，且图必须无环。背压会自动只在
**汇聚（sink）** 消费者（没有其他消费者依赖它们）上设闸，因为它们的游标总是
落后于任何祖先。

## work-pool（负载均衡消费）

消费者是广播——每个事件被每个 handler 看到；而 `WorkerPool` 做负载均衡：每个事件
只被一个 worker 处理。用它把独立的逐事件工作并行到多核。

```go
pool := d.WorkerPool(runtime.NumCPU(), func(buf []Event, mask, seq int64) {
    _ = buf[seq&mask] // 处理单个事件
})
d.RegisterWorkerPool(pool)
d.Start()
```

pool 作为 sink 对生产者设闸（任一 worker 仍在处理的槽位绝不会被覆盖），关闭时像
消费者一样排空。加 `OnPanic` 可做逐事件 panic 恢复。

## 等待策略

`WithWaitStrategy` 选择等待者如何自旋；它同时作用于消费者和生产者背压。

| 策略            | 延迟   | CPU  | 说明                                  |
|-----------------|--------|------|---------------------------------------|
| `BusySpinWait`  | 最低   | 高   | 独占一个核；仅在有空闲核时使用         |
| `YieldingWait`  | 低     | 中   | **默认** —— 先自旋，再把 P 让给调度器  |
| `SleepingWait`  | 较高   | 低   | 自旋 → 让出 → 短睡；适合低频/空闲流    |

```go
d := disruptor.NewDisruptor(1024, newEvent,
    disruptor.WithWaitStrategy(disruptor.BusySpinWait{}))
```

## panic 处理

默认情况下，事件处理器中的 panic 会让该消费者的 goroutine 崩溃，从而卡住整条
流水线。安装 `Consumer.OnPanic`（或 `WorkerPool.OnPanic`）即可恢复并继续，或用
全局 `WithPanicHandler` 设一个被所有消费者继承的默认处理器——长期运行的服务
推荐设置。

```go
c := d.Consumer(handler).OnPanic(func(recovered any, lo, hi int64) {
    log.Printf("处理器在 [%d,%d] 上 panic：%v", lo, hi, recovered)
})
```

## 生命周期钩子

`OnStart` / `OnShutdown` 在各 stage 自己的 goroutine 中运行——`OnStart` 在开始
处理之前，`OnShutdown` 在排空之后、`Stop` 返回之前——用于逐 goroutine 的初始化
与清理。对 worker pool 而言，它们按每个 worker 各触发一次。

```go
c := d.Consumer(handler).
    OnStart(func() { /* 预热缓存、注册指标 */ }).
    OnShutdown(func() { /* flush、释放资源 */ })
```

## 关闭契约

`Stop` 是「先排空、再唤醒」：它先等待每个消费者追上最后一个已发布序号，然后
唤醒各 barrier，让消费者 goroutine 退出。任何已发布的事件都不会丢失。

1. 停止发布 —— 应用必须停止调用 `Next`/`Publish`。
2. 调用 `Stop`。

**不要**在生产者仍可能发布时调用 `Stop`，也**不要**在生产者阻塞于满环的 `Next`
时调用 `Stop` —— `Stop` 不会唤醒生产者。需要保持对关闭响应的生产者，应改用
`TryNext` 或 `NextContext`。

`StopContext(ctx)` 是有界版本：若某消费者卡死，它在 deadline 返回 `ctx.Err()`，
而不是永久阻塞。

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := d.StopContext(ctx); err != nil {
    log.Printf("关闭未能干净排空：%v", err)
}
```

## 可观测性

`Stats()` 是廉价的拉取式快照，只读原子游标、不碰热路径，适合周期性采集：

```go
s := d.Stats()
// s.Published、s.Capacity、s.Free、s.Backpressure、
// s.ConsumerLag[i]、s.WorkerPoolLag[i]
```

`WithMetrics(interval, sink)` 启动后台采样器，每隔 interval 把一份 `Stats` 推给
`sink`（关闭时再推最后一份）：

```go
d := disruptor.NewDisruptor(1024, newEvent,
    disruptor.WithMetrics(time.Second, func(s disruptor.Stats) {
        log.Printf("published=%d free=%d backpressure=%d lag=%v",
            s.Published, s.Free, s.Backpressure, s.ConsumerLag)
    }))
```

## 并发规则

- 任何 `Next`/`Publish` 之前必须先 `Start`。
- 单生产者下，只能有一个 goroutine 调用 `Next`/`Publish`。
- 多生产者下，任意数量的 goroutine 均可调用。
- 在 `Start` 之前完成消费者配置（`Depends`/`OnPanic`）。
- 切勿按值拷贝 `Sequence`、`RingBuffer` 或 `Disruptor` —— 一律传指针
  （`go vet` 会标记对内嵌原子量的误拷贝）。

## 设计说明

- **Sequence** 是内嵌 `atomic.Int64` 的单调递增游标，两侧填充，使热点游标永不
  与相邻字段共享同一缓存行（伪共享）。
- **单写者发布** 是对游标的一次普通原子存储 —— 因为槽位严格按序认领，不会出现
  空洞。
- **多写者发布** 给每个槽位打上其圈数（`seq >> shift`）标记，使上一圈的陈旧值
  绝不会被误认成本圈的新发布；随后多个生产者协作地把发布游标推进到最高的连续
  序号，从而生产者之间不会因彼此乱序提交而互相卡住。
- **Barrier** 聚合一组上游序号并暴露它们的最小值作为单一闸口，既用于消费者
  进度，也用于生产者背压。

完整 API 见包文档（`go doc`）。

## 测试

```sh
go test -race ./...                        # 正确性，含 DAG + 排空 + 背压
go test -run '^$' -bench . -benchmem ./...  # 基准
```

## 许可证

基于 [MIT 许可证](LICENSE) 发布。© 2026 gagral。
