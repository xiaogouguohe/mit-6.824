package kvraft

import (
	"6.824_new/src/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	opUni OpUnique
	opUniMu sync.RWMutex
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
	ck.name = randstring(20)
	ck.opUniMu.Lock()
	ck.opUni = OpUnique{
		ClerkName: ck.name,
		Seq:       100,
	}
	//fmt.Println("in func MakeClerk, opUni:", ck.opUni)
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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//return ""

	//atomic.AddInt32(&ck.seq, 1) //还是有并发问题
	ck.opUniMu.Lock()
	opUni := ck.opUni
	ck.opUni.Seq++
	ck.opUniMu.Unlock()
	//fmt.Println("in func client's Get, opUni:", opUni)
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		args := GetArgs{
			Key: key,
			OpUni: opUni,
		}
		var reply GetReply
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	//fmt.Println("in func client's PutAppend, len(ck.servers):", len(ck.servers))
	//atomic.AddInt32(&ck.seq, 1) //还是有并发问题
	ck.opUniMu.Lock()
	opUni := ck.opUni
	ck.opUni.Seq++
	ck.opUniMu.Unlock()
	//fmt.Println("in func client's PutAppend, opUni:", opUni)
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			OpUni:   opUni,

		}
		//fmt.Println("in func client's PutAppend, seq:", ck.seq)
		var reply PutAppendReply
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		//fmt.Println("in func client's PutAppend, ok:", ok, "server:", i, "Key:", key, "Value:", value, "Op:", op, "opUni:", opUni,
			//"reply.Err:", reply.Err)
		if ok && reply.Err == OK {
			//fmt.Println("in func client's PutAppend, i:", i, "ok and reply.Err is nil")
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
