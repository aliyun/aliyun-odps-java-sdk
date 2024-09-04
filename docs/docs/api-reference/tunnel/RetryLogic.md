---
title: 重试逻辑
sidebar_position: 2
---

# RetryPolicy 重试策略

`odps-sdk` 使用 `TunnelRetryHandler` 类用于处理 ODPS Tunnel 服务中的重试策略。
在现实环境中，由于网络原因，服务端限流策略等多种原因，tunnel 服务可能出现多种错误，`odps-sdk`
力图提供一种通用的重试机制，让用户能够对非客户端问题无感知。

## 重试策略说明

- **429**: 服务器端限流策略，无限指数重试（最大等待时间为64秒）。
- **502, 504**: 触发会话重新加载并进行指数重试（1秒到64秒）。
- **5xx**: 服务器端错误，进行指数重试（1秒到64秒）。
- **308**: 仅用于 `UpsertSession`，更新客户端缓存 ip 地址，并进行无限指数重试（最大等待时间为64秒）。
- **其他**: 其他错误，通常为客户端问题，使用用户配置的重试逻辑（默认不重试）。

## 如何配置重试逻辑

目前，tunnel 仅支持配置上述其他错误的重试逻辑，而被配置过的重试逻辑是固定的。（实际上，被配置过的错误都属于用户“本不应当察觉”的错误，可以安全重试）
用户应当实现 [`RetryPolicy`](#retrypolicy-接口) 接口，并通过 [`Configuration.Builder#withRetryPolicy`](Configuration.md#设置重试策略) 方法进行配置。

## 打印重试日志

由于 `odps-sdk` 内置了重试逻辑，我们注意到用户在使用 tunnel 时，需要打印重试日志，以便于排查问题。
用户可以通过实现 [`RetryLogger`](#retrylogger) 接口，并通过 [`Configuration.Builder#withRetryLogger`](Configuration.md#设置重试日志记录器) 方法进行配置。


## RetryPolicy 接口

`RetryPolicy` 接口定义了重试策略的基本方法。

### 方法

#### 是否应该重试

```java
boolean shouldRetry(Exception e, int attempt)
```

根据异常和重试次数决定是否进行重试。

#### 获取重试等待时间

```java
long getRetryWaitTime(int attempt)
```

根据重试次数获取等待时间。


## RetryLogger 接口

`RetryLogger` 是一个抽象类，用于在 `RestClient` 发生重试前进行日志记录或其他操作。通过实现该类，用户可以在每次重试前执行自定义的逻辑，例如记录重试信息、发送通知等。

### 方法

#### onRetryLog

```java
public abstract void onRetryLog(Throwable e, long retryCount, long retrySleepTime);
```

当 `RestClient` 发生重试前的回调函数。

##### 参数

- `e`：错误异常，表示导致重试的异常。
- `retryCount`：重试计数，表示当前是第几次重试。
- `retrySleepTime`：下次需要的重试时间（秒），表示在进行下一次重试前需要等待的时间。

### 使用示例

用户可以通过继承 `RetryLogger` 抽象类并实现 `onRetryLog` 方法，来定义自定义的重试日志记录逻辑。例如：

```java
public class CustomRetryLogger extends RetryLogger {
    @Override
    public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
        System.out.println("Retry attempt #" + retryCount + " after " + retrySleepTime + " seconds due to: " + e.getMessage());
    }
}
```