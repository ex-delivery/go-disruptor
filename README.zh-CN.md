# go-disruptor

[English](README.md) | **简体中文**

一个面向超低延迟生产者/消费者流水线的泛型、无锁环形缓冲区，仿照
[LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)（v4 事件模型）设计。

热路径零分配：槽位是预先分配、随序号推进而复用的 `T` 值。协调通过按缓存行
隔离的原子游标完成，不使用 channel 或互斥锁。

```
goos: darwin  goarch: arm64  cpu: Apple M4  (Go 1.26.2)

# 吞吐 — 0 allocs/op
BenchmarkSPSC            ~6   ns/op    # disruptor：1 生产者 -> 1 消费者
BenchmarkMPSC          ~240  ns/op    # disruptor：4 生产者 -> 1 消费者
BenchmarkChannelSPSC    ~25  ns/op    # buffered channel，对照

# 端到端延迟（发布 -> 消费），代表值
BenchmarkLatencySPSC          p50 ~7µs   p99 ~27µs    # disruptor
BenchmarkChannelLatencySPSC   p50 ~41µs  p99 ~147µs   # buffered channel
```

## 特性

- **泛型** —— `Disruptor[T]` 支持任意值类型；无 `interface{}`、无装箱。
- **热路径零分配** —— 槽位一次性预建，发布/消费复用同一批对象。
- **Disruptor v4 事件模型** —— `EventHandler.OnEvent(event, sequence, endOfBatch)`
  逐事件调用；`EventHandlerFunc` 可把普通函数适配为 handler。
- **单生产者与多生产者** —— 可选极速的单写者路径，或基于 CAS 的多写者定序器。
- **阻塞 / 非阻塞 / 可取消认领** —— `Next` 施加背压；`TryNext` 满环立即失败；
  `NextContext` 阻塞但可被 ctx 取消。
- **DAG 消费者图** —— 消费者可依赖其他消费者，构建并行阶段与扇出/扇入拓扑。
- **批量回退（rewind）** —— handler 返回 `ErrRewind` 可重处理整个批次，由
  `BatchRewindStrategy` 控制；`MaxBatchSize` 可限制单批大小。
- **可选 handler 钩子** —— 实现 `LifecycleAware`（OnStart/OnShutdown）、
  `BatchStartAware`（OnBatchStart）或 `TimeoutAware`（OnTimeout）。
- **可插拔等待策略** —— 在延迟与 CPU 之间权衡：忙等、让出（默认）、休眠。
- **有界优雅关闭** —— `Stop` 先排空全部已发布事件；`StopContext` 带超时，卡死的
  消费者也无法挂起关闭。
- **v4 式异常处理** —— 把 handler 的错误/panic 路由到逐消费者或全局的
  `ExceptionHandler`。
- **可观测性** —— 拉取式 `Stats`（lag、占用、背压计数）加可选 `WithMetrics`
  采样，均不碰热路径。
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

// handler 逐事件调用。EventHandlerFunc 适配普通函数；也可用实现 EventHandler
// 的结构体，并额外实现下文的可选钩子。
c := d.Consumer(disruptor.EventHandlerFunc[Event](
    func(e *Event, seq int64, endOfBatch bool) error {
        _ = e // 处理事件
        return nil
    }))
d.RegisterConsumer(c)
d.Start()
defer d.Stop()

seq := d.Next(1)        // 认领一个槽位（背压下会阻塞）
d.Get(seq).Value = 42   // 就地填充
d.Publish(seq, seq)     // 对消费者可见
```

`NewDisruptor` 会把容量向上取整到 2 的幂，从而用 `seq & mask` 代替取模。

## 多生产者

传入 `WithProducerFunc(NewMultiProducer)` 允许多个写者 goroutine 并发写入。
默认的 `NewSingleProducer` 更快，但只对恰好一个写者安全。

```go
d := disruptor.NewDisruptor(1024, newEvent,
    disruptor.WithProducerFunc(disruptor.NewMultiProducer))
```

## 非阻塞与可取消认领

`Next` 会（按等待策略）阻塞直到最慢的消费者腾出空间；`TryNext` 在满环时立即
返回 `(0, false)`；`NextContext` 阻塞但在 ctx 取消时返回 `ctx.Err()`——后两者
让生产者能响应关闭（`Stop` 无法唤醒 `Next`）。

```go
if seq, ok := d.TryNext(1); ok {
    d.Get(seq).Value = 42
    d.Publish(seq, seq)
}

seq, err := d.NextContext(ctx, 1) // 取消时 err == ctx.Err()
free := d.RemainingCapacity()     // 当前空闲槽位数（估算）
```

## 依赖图（DAG）

一个消费者可以依赖其他消费者，只在它们处理过某序号之后才处理该序号：

```go
b := d.Consumer(handlerB)
c := d.Consumer(handlerC)
merge := d.Consumer(handlerMerge).Depends(b, c) // 仅在 b 和 c 之后运行
d.RegisterConsumer(b, c, merge)
```

所有 `Depends` 连线必须在 `Start` 之前完成，且图必须无环。背压会自动只在
**汇聚（sink）** 消费者上设闸。

## 异常处理

`OnEvent` 返回的非 rewind 错误，或其中的 panic，会被路由到该消费者的
`ExceptionHandler`；生命周期回调中的 panic 走 `HandleOnStartException` /
`HandleOnShutdownException`。用 `HandleExceptionsWith` 逐消费者设置，或用
`Disruptor.HandleExceptionsWith` 设全局默认。都不设时，返回的错误被忽略、panic
向上传播（fail-fast）——长期运行的服务请设置一个。

```go
c := d.Consumer(handler).HandleExceptionsWith(myExceptionHandler)
```

## 批量回退与最大批量

handler 返回 `ErrRewind` 可请求从批次首序号重处理整个批次（handler 须对重处理
幂等）。用 `BatchRewindStrategy` 启用：

```go
c := d.Consumer(handler).
    WithRewindStrategy(disruptor.GiveUpAfter{Max: 3}). // 或 AlwaysRewind{}
    MaxBatchSize(256)                                  // 限制单批事件数
```

未设策略时，`ErrRewind` 等同普通错误处理。

## 生命周期与超时钩子

在 handler 上实现可选接口，处理器会自动检测（Disruptor v4 把它们作为默认方法
并入 `EventHandler`）：

```go
type Handler struct{}
func (Handler) OnEvent(e *Event, seq int64, endOfBatch bool) error { return nil }
func (Handler) OnStart()                       {} // LifecycleAware
func (Handler) OnShutdown()                    {} // 在 Stop 返回前执行
func (Handler) OnBatchStart(size, depth int64) {} // BatchStartAware
func (Handler) OnTimeout(seq int64)            {} // TimeoutAware（需 Timeout）
```

设置 `Consumer.Timeout(d)` 后，空闲超过 d 且无新事件时触发 `OnTimeout`。

## 等待策略

`WithWaitStrategy` 选择等待者如何自旋；同时作用于消费者和生产者背压。

| 策略            | 延迟   | CPU  | 说明                                  |
|-----------------|--------|------|---------------------------------------|
| `BusySpinWait`  | 最低   | 高   | 独占一个核；仅在有空闲核时使用         |
| `YieldingWait`  | 低     | 中   | **默认** —— 先自旋，再把 P 让给调度器  |
| `SleepingWait`  | 较高   | 低   | 自旋 → 让出 → 短睡；适合低频/空闲流    |

## 关闭契约

`Stop` 是「先排空、再唤醒」：先等待每个消费者追上最后一个已发布序号，再唤醒
barrier 让 goroutine 退出。任何已发布事件都不会丢失。

1. 停止发布 —— 应用必须停止调用 `Next`/`Publish`。
2. 调用 `Stop`（或 `StopContext`）。

**不要**在生产者仍可能发布、或阻塞于满环 `Next` 时调用 `Stop` —— 它不会唤醒
生产者。`StopContext(ctx)` 是有界版本：消费者卡死时在 deadline 返回 `ctx.Err()`
而非永久挂起。

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := d.StopContext(ctx); err != nil {
    log.Printf("关闭未能干净排空：%v", err)
}
```

## 可观测性

`Stats()` 是廉价的拉取式快照，只读原子游标、不碰热路径，适合周期采集：

```go
s := d.Stats()
// s.Published、s.Capacity、s.Free、s.Backpressure、s.ConsumerLag[i]
```

`WithMetrics(interval, sink)` 启动后台采样器，每隔 interval 把一份 `Stats` 推给
`sink`（关闭时再推最后一份）。

## 并发规则

- 任何 `Next`/`Publish` 之前必须先 `Start`。
- 单生产者下，只能有一个 goroutine 调用 `Next`/`Publish`。
- 多生产者下，任意数量的 goroutine 均可调用。
- 在 `Start` 之前完成消费者配置（`Depends` / `HandleExceptionsWith` /
  `WithRewindStrategy` / `MaxBatchSize` / `Timeout`）。
- 切勿按值拷贝 `Sequence`、`RingBuffer` 或 `Disruptor` —— 一律传指针。

## 设计说明

- **Sequence** 内嵌 `atomic.Int64`，两侧填充，使热点游标永不与相邻字段共享同一
  缓存行（伪共享）。
- **单写者发布** 是对游标的一次普通原子存储；**多写者发布** 给每个槽位打上圈数
  标记，使上一圈的陈旧值不会被误认成新发布，随后协作地把发布游标推进到最高的
  连续序号。
- **Barrier** 聚合上游序号并暴露其最小值作为单一闸口，既用于消费者进度，也用于
  生产者背压。

## 与 Disruptor v4 的关系

本项目是 Disruptor **v4** 事件模型的 Go 移植：逐事件 `OnEvent` + `endOfBatch`
标志、生命周期/批次开始/超时并入 handler、批量回退、最大批量。与 v4 一致，**不**
包含旧的 `WorkerPool`/`WorkHandler`。API 采用 Go 惯用法（接口、error、`context`），
而非逐字直译。

## 测试

```sh
go test -race ./...                        # 正确性，含 DAG + 排空 + rewind
go test -run '^$' -bench . -benchmem ./...  # 基准
```

## 许可证

基于 [MIT 许可证](LICENSE) 发布。© 2026 gagral。
