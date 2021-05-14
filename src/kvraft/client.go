package kvraft

import (
	"6.824_new/src/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

/* 发起请求的客户端 */
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	/* 这个客户端的当前请求 */
	opUni OpUnique
	opUniMu sync.RWMutex
	/* 客户端的唯一标识 */
	name string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	/* 给客户端起名 */
	ck.name = randstring(20)
	ck.opUniMu.Lock()
	/* 客户端的请求序列从100开始 */
	ck.opUni = OpUnique{
		ClerkName: ck.name,
		Seq:       100,
	}
	ck.opUniMu.Unlock()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
/* 客户端发起 Get 请求 */
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//return ""

	ck.opUniMu.Lock()
	opUni := ck.opUni
	/* 客户端请求序列 +1 */
	ck.opUni.Seq++
	ck.opUniMu.Unlock()
	/* 客户端轮训每个服务器节点 */
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		args := GetArgs{
			Key: key,
			OpUni: opUni,
		}
		reply := GetReply{
			Err:   "initial",
			Value: "",
		}
		ok := false
		okCh := make(chan bool, 1)
		go func() {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			okCh <- ok
		}()

		/* 等 500 ms, 如果还没有ok，视为 rpc 超时*/
		select{
		case ok = <- okCh:

		case <- time.After(500 * time.Millisecond):

		}

		/* rpc 调用不超时，且 Get 正确值 */
		if ok && reply.Err == OK {
			return reply.Value
		}
	}
	//return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
/* 参考上面的 Get */
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	//atomic.AddInt32(&ck.seq, 1) //还是有并发问题
	ck.opUniMu.Lock()
	opUni := ck.opUni
	ck.opUni.Seq++
	ck.opUniMu.Unlock()
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			OpUni:   opUni,

		}
		reply := PutAppendReply{
			Err: "initial",
		}
		ok := false
		okCh := make(chan bool, 1)
		go func() {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			okCh <- ok
		}()

		select{
		case ok = <- okCh:

		case <- time.After(500 * time.Millisecond):

		}
		if ok && reply.Err == OK {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
