package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpUni OpUnique

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpUni OpUnique
}

type GetReply struct {
	Err   Err
	Value string
}

/* 一次客户端请求的唯一标识 */
type OpUnique struct {
	/* 客户端名字 */
	ClerkName string
	/* 请求的序列号，在每个客户端中依次递增 */
	Seq int32
}