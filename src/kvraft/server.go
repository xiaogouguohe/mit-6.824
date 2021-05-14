package kvraft

import (
	"6.824_new/src/labgob"
	"6.824_new/src/labrpc"
	"6.824_new/src/raft"

	//"fmt"

	"time"

	//"../labgob"
	//"../labrpc"
	"log"
	//"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key string
	Value string
	Ope string
	OpUni OpUnique

}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//data map[string]string
	//dataMu sync.RWMutex
	data sync.Map


	putAppendChs sync.Map
	putAppendOps sync.Map
}

/* 被客户端的 Get 发起 rpc 调用 */
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	/* 调用 raft 的 Start，把 Op 写到日志里 */
	_, _, succ := kv.rf.Start(Op{
		Key: args.Key,
		Ope: "Get",
		OpUni: args.OpUni,
	})

	/* 写入成功 */
	if succ {
		/* 取出当前 OpUni 对应的通道 */
		val, ok := kv.putAppendChs.Load(args.OpUni)
		/* 还未创建通道 */
		if !ok {
			kv.putAppendChs.Store(args.OpUni, make(chan raft.ApplyMsg, 1024))
			val, _ = kv.putAppendChs.Load(args.OpUni)
		}
		ch := val.(chan raft.ApplyMsg)
		/* 标记这个 OpUni 为已写入通道，防止重复写入 */
		// kv.putAppendOps.Store(args.OpUni, 1)

		select{
		case <- ch:
			reply.Err = OK
			value, ok := kv.data.Load(args.Key)
			if ok {
				reply.Value = value.(string)
			}

		case <- time.After(time.Duration(400 * time.Millisecond)):
			reply.Err = "time out"
		}

	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, _, succ := kv.rf.Start(Op{
		Key: args.Key,
		Value: args.Value,
		Ope: args.Op,
		OpUni: args.OpUni,

	})
	if succ {
		val, ok := kv.putAppendChs.Load(args.OpUni)
		if !ok {
			kv.putAppendChs.Store(args.OpUni, make(chan raft.ApplyMsg, 1024))
			val, _ = kv.putAppendChs.Load(args.OpUni)
		}
		ch := val.(chan raft.ApplyMsg)
		select{
		case <- ch:
			reply.Err = OK

		case <- time.After(time.Duration(500 * time.Millisecond)):
			reply.Err = "time out"
		}
	} else {
		reply.Err = ErrWrongLeader
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
/* 启动服务器 */
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// kv.data = make(map[string]string)
	kv.data = sync.Map{}
	kv.putAppendChs = sync.Map{}
	kv.putAppendOps = sync.Map{}

	logs := kv.rf.GetLogsBeforeCommitIndex(me)
	// kv.dataMu.Lock()
	for i := 0; i < len(logs); i++ {
		log := logs[i]
		op, _ := log.Command.(Op)
		if op.Ope == "Put" {
			kv.data.Store(op.Key, op.Value)
		} else if op.Ope == "Append" {
			oriValue, ok := kv.data.Load(op.Key)
			if ok {
				kv.data.Store(op.Key, oriValue.(string) + op.Value)
			} else {
				kv.data.Store(op.Key, op.Value)
			}

		}
	}

	/* 这个 goroutine 接收 raft 提交过来的日志 */
	go func() {
		for {
			applyMsg := <- kv.applyCh
			op, _ := applyMsg.Command.(Op)

			/* 取出 op 对应的通道 */
			_, ok := kv.putAppendChs.Load(op.OpUni)
			/* 之前还没有创建 op 对应的通道，先创建通道 */
			if !ok {
				kv.putAppendChs.Store(op.OpUni, make(chan raft.ApplyMsg, 1024))
			}


			/* 之前已经把 op 写入通道了 */
			if _, ok := kv.putAppendOps.Load(op.OpUni); ok {
			} else {
				/* 标记 op 已经被写入通道了，以后不要再写了 */
				kv.putAppendOps.Store(op.OpUni, 1)

				if op.Ope == "Put" {
					kv.data.Store(op.Key, op.Value)
				} else if op.Ope == "Append" {
					oriValue, ok := kv.data.Load(op.Key)
					if ok {
						kv.data.Store(op.Key, oriValue.(string) + op.Value)
					} else {
						kv.data.Store(op.Key, op.Value)
					}
				}

			}

			if kv.rf.GetCertainState() == raft.LEADER {
				ch ,_ := kv.putAppendChs.Load(op.OpUni)
				ch.(chan raft.ApplyMsg) <- applyMsg
			}
		}
	}()

	return kv
}
