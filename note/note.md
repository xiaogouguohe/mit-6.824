1. 解决 UnreliaableFigure8 的一种方法，注意更新选举超时定时器，也就是 UpdateLastReceivedTime 的时机
    - 发起选举
    - 投票（只有投出去选票）
    - 收到任期没有过期的附加日志 RPC
2. 解决 UnreliaableFigure8 的一种方法，注意附加日志 RPC，什么时候才可以 reply true，看代码
3. Lab 3 用 sync.Map 代替了 map，注意 map op 是用来统计这个 op 有没有被放到通道里，放置重复放置
