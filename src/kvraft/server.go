package kvraft

import (
	"6.824_new/src/labgob"
	"6.824_new/src/labrpc"
	"6.824_new/src/raft"
	"bytes"
	"fmt"

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
	data map[string]string
	dataMu sync.RWMutex

	putAppendChs map[OpUnique]chan raft.ApplyMsg
	putAppendOps map[OpUnique]bool
	putAppendChsMu sync.RWMutex
	putAppendOpsMu sync.RWMutex

	applyIndex int32
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Println("in func kv.Get, Key:", args.Key)

	_, _, succ := kv.rf.Start(Op{
		Key: args.Key,
		//Value: args.Value,
		Ope: "Get",
		OpUni: args.OpUni,
	})

	if succ {
		kv.putAppendChsMu.Lock()
		if _, ok := kv.putAppendChs[args.OpUni]; ok {
			kv.putAppendChsMu.Unlock()
		} else {
			kv.putAppendChs[args.OpUni] = make(chan raft.ApplyMsg, 1024)
			//kv.putAppendOps[args.OpUni] = true
			kv.putAppendChsMu.Unlock()
		}

		//fmt.Println("in func server's Get, kv:", kv.me, "OpUni:", args.OpUni, "try to get from channel")

		select{
		case <- kv.putAppendChs[args.OpUni]:
			//fmt.Println("in func server's Get, kv:", kv.me, "OpUni:", args.OpUni, "get from channel")
			reply.Err = OK
			kv.dataMu.Lock()
			//fmt.Println("in func server's Get, kv:", kv.me, "key:", args.Key, "Value:", kv.data[args.Key])
			reply.Value = kv.data[args.Key]
			//fmt.Println("in func server's Get, reply.Value:", reply.Value)
			kv.dataMu.Unlock()

		case <- time.After(time.Duration(400 * time.Millisecond)):
			//fmt.Println("in func server's Get, time out, args:", args)
			reply.Err = "time out"
		}
		/*applyMsg := *///<- kv.putAppendChs[args.OpUni]
		//fmt.Println("in func server's Get, get from putAppendCh, applyMsg:", applyMsg)
		//fmt.Println("in func server's Get, OpUni:", args.OpUni)

	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("in func server's PutAppend, Key:", args.Key, "Value:", args.Value, "OP:", args.Op)
	//time.Sleep(1 * time.Second)

	//fmt.Println("in func kv.PutAppend, Key:", args.Key, "Value:", args.Value)

	_, _, succ := kv.rf.Start(Op{
		Key: args.Key,
		Value: args.Value,
		Ope: args.Op,
		OpUni: args.OpUni,

	})
	if succ {
		kv.putAppendChsMu.Lock()
		if _, ok := kv.putAppendChs[args.OpUni]; ok {
			kv.putAppendChsMu.Unlock()
		} else {
			kv.putAppendChs[args.OpUni] = make(chan raft.ApplyMsg, 1024)
			//kv.putAppendOps[args.OpUni] = true
			kv.putAppendChsMu.Unlock()
		}

		//fmt.Println("in func server's putAppend, kv:", kv.me, "OpUni:", args.OpUni, "try to get from channel")

		select{
		case <- kv.putAppendChs[args.OpUni]:
			//fmt.Println("in func server's putAppend, kv:", kv.me, "OpUni:", args.OpUni, "get from channel")
			reply.Err = OK

		case <- time.After(time.Duration(500 * time.Millisecond)):
			//fmt.Println("in func server's PutAppend, time out, args:", args)
			reply.Err = "time out"
		}
		//fmt.Println("in func server's PutAppend, get from putAppendCh, applyMsg:", applyMsg)
		/*if args.Op == "Put" {
			kv.dataMu.Lock()
			kv.data[args.Key] = args.Value
			kv.dataMu.Unlock()
		} else {
			//fmt.Println("in func test, is Append, Key:", args.Key, "kv.data[args.Key]:", kv.data[args.Key], "args.Value", args.Value, "before add")
			kv.dataMu.Lock()
			kv.data[args.Key] += args.Value
			//fmt.Println("in func test, is Append, Key:", args.Key, "kv.data[args.Key]:", kv.data[args.Key], "args.Value", args.Value, "after add")
			kv.dataMu.Unlock()
		}*/
	} else {
		//fmt.Println("in func server's PutAppend, false")
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	//fmt.Println("in func StartKVServer, me:", me)
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.putAppendChs = make(map[OpUnique]chan raft.ApplyMsg)
	kv.putAppendOps = make(map[OpUnique]bool)

	logs := kv.rf.GetLogsBeforeCommitIndex(me)
	kv.applyIndex = -1

	kv.dataMu.Lock()

	if maxraftstate > -1 {
		dbdata := persister.ReadSnapshot()


		if dbdata == nil || len(dbdata) < 1 { // bootstrap without any state?
			//return
		} else {
			r := bytes.NewBuffer(dbdata)
			d := labgob.NewDecoder(r)

			if d.Decode(&kv.data) != nil {
				//fmt.Println("in func readPersist, decode error")
				//return
			} else {

				//fmt.Println("in func readPersist, after decode, term:", rf.GetCurrentTerm(), "votedFor:", rf.GetVotedFor(), "logs:", rf.GetLogs(0))
			}
		}
	}

	for i := 0; i < len(logs); i++ {
		log := logs[i]
		op, _ := log.Command.(Op)
		if op.Ope == "Put" {
			kv.dataMu.Lock()
			kv.data[op.Key] = op.Value
			kv.dataMu.Unlock()
		} else if op.Ope == "Append" {
			kv.dataMu.Lock()
			kv.data[op.Key] += op.Value
			kv.dataMu.Unlock()
		}
	}
	kv.dataMu.Unlock()

	go func() {
		for {
			if kv.killed() {
				break
			}
			applyMsg := <- kv.applyCh
			op, _ := applyMsg.Command.(Op)
			kv.dataMu.Lock()
			atomic.StoreInt32(&kv.applyIndex, int32(applyMsg.CommandIndex) - 1)
			kv.dataMu.Unlock()
			//在这里做判断，不要重复添加
			kv.putAppendChsMu.Lock()
			/*fmt.Println("in func StartKVServer, op:", op)
			for k, v := range(kv.putAppendOps) {
				fmt.Println("in func StartKVServer, kv:", kv.me, "key:", k, "value", v)
			}*/
			if _, ok := kv.putAppendChs[op.OpUni]; ok {
				//fmt.Println("in func StartKVServer, kv:", kv.me, "op.Opuni:", op.OpUni, "already exist")
				//continue
				kv.putAppendChsMu.Unlock()
			} else {

				 //fmt.Println("in func StartKVServer, kv:", kv.me, "op.Opuni:", op.OpUni, "set")

				kv.putAppendChs[op.OpUni] = make(chan raft.ApplyMsg, 1024)
				//kv.putAppendOps[op.OpUni] = true
				kv.putAppendChsMu.Unlock()

			}

			kv.putAppendOpsMu.Lock()
			if _, ok := kv.putAppendOps[op.OpUni]; ok {
				kv.putAppendOpsMu.Unlock()
			} else {
				kv.putAppendOps[op.OpUni] = true
				kv.putAppendOpsMu.Unlock()

				if op.Ope == "Put" {
					kv.dataMu.Lock()
					kv.data[op.Key] = op.Value
					kv.dataMu.Unlock()
				} else if op.Ope == "Append" {
					//fmt.Println("in func StartKVServer, is Append, kv:", kv.me, "Key:", op.Key, "kv.data[args.Key]:", kv.data[op.Key], "args.Value", op.Value, "before add")
					kv.dataMu.Lock()
					kv.data[op.Key] += op.Value
					//fmt.Println("in func StartKVServer, is Append, kv:", kv.me, "Key:", op.Key, "kv.data[args.Key]:", kv.data[op.Key], "args.Value", op.Value, "after add")
					kv.dataMu.Unlock()
				}

			}

			if kv.rf.GetCertainState() == raft.LEADER {
				//fmt.Println("in func start a server, put applyMsg into putAppendCh, applyMsg:", applyMsg)
				//fmt.Println("in func StartKVServer, kv:", kv.me, "OpUni:", op.OpUni, "applyMsg:", applyMsg, "put to channel")
				kv.putAppendChs[op.OpUni] <- applyMsg

				/*go func() {
					time.Sleep(500 * time.Millisecond)
					<- kv.putAppendChs[op.OpUni]
				}()*/

			}

		}
	}()
	if maxraftstate > -1 {
		go func() {
			for {
				if kv.killed() {
					break
				}
				fmt.Println("in func snapshot, server:", kv.me, "snapshot's size:", persister.RaftStateSize())
				if maxraftstate <= persister.RaftStateSize() {
					kv.dataMu.RLock()
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)

					e.Encode(kv.data)

					applyIndex := kv.applyIndex
					kv.dataMu.RUnlock()

					dbdata := w.Bytes()
					//kv.rf.PersistFromTo(kv.rf.GetAfterSnapshotIndex(), kv.rf.GetLastLogIndex() + 1)

					w2 := new(bytes.Buffer)
					e2 := labgob.NewEncoder(w2)
					afterSnapshotIndex := kv.rf.GetAfterSnapshotIndex()
					kv.rf.SetAfterSnapshotIndex(applyIndex + 1)
					logs := kv.rf.GetLogsFromTo(afterSnapshotIndex, applyIndex + 1)
					e2.Encode(kv.rf.GetCurrentTerm())
					e2.Encode(kv.rf.GetVotedFor())
					e2.Encode(logs)
					rfdata := w2.Bytes()
					persister.SaveStateAndSnapshot(dbdata, rfdata)


				}
				time.Sleep(10 * time.Millisecond)
			}

		}()
	}

	return kv
}
