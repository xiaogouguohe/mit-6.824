package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824_new/src/labgob"
	"fmt"

	//"src/labgob"
	"6.824_new/src/labrpc"
	"bytes"
	"encoding/gob"
	//"fmt"
	"math/rand"

	"sync/atomic"

	"sync"
	"time"
)

//import "../labrpc"

// import "bytes"
// import "../labgob"

var VOTE_NIL = -1
var RPC_CALL_TIMEOUT = 1 * time.Second
var HEARTBEAT_INTERVAL = 100 * time.Millisecond
var LEADER uint32 = 1
var CANDIDATE uint32 = 2
var FOLLOWER uint32 = 3

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor int
	state uint32
	//leader uint32

	commitIndex int32
	lastApplied int32

	nextIndex []int32
	matchIndex []int32

	logs []LogEntry

	applyCh chan ApplyMsg
	
	heartbeatInterval time.Duration
	electionInterval time.Duration

	voteMu sync.RWMutex
	nextIndexMu sync.RWMutex
	matchIndexMu sync.RWMutex
	logsMu sync.RWMutex

	lastRecvTime int64

	cmdCh chan int32

	afterSnapshotIndex int32
	lastIncludeTerm int32
}

type LogEntry struct {
	Term int
	Command interface{}
}

func Encode(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {    //返回这个raft当前的状态以及是否为leader

	var term int
	var isleader bool

	// Your code here (2A).
	term, _ = rf.GetVoteState()
	state := rf.GetCertainState()
	if state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return int(term), isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//fmt.Println("in func persist, begin, rf:", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//term, votedFor := rf.GetVoteState()
	//logs := rf.GetLogs(0)

	//fmt.Println("in func persist, me:", rf.me, "currentTerm:", rf.currentTerm, "votedFor:", rf.votedFor, "lastApplied:", rf.lastApplied, "logs:", rf.logs)
	if int(rf.lastApplied) >= len(rf.logs) {
		//fmt.Println("in func persist, me:", "lastApplied:", rf.lastApplied, "len of logs:", len(rf.logs))
	}

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastApplied)
	e.Encode(rf.afterSnapshotIndex)
	e.Encode(rf.lastIncludeTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//rf.persister = rf.persister.Copy()

	/*//fmt.Println("in func persist, test decode begin")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []LogEntry
	var lastApplied int32
	var afterSnapshotIndex int32

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastApplied) != nil || d.Decode(&afterSnapshotIndex) != nil {
		fmt.Println("in func persist, decode error")
		fmt.Println("in func persist, me:", rf.me, "test eecode, term:", term, "votedFor", votedFor, "lastApplied:", lastApplied, "afterSnaoshotIndex:", afterSnapshotIndex, "logs:", logs, "decode error")
	} else if lastApplied == 0 {
		fmt.Println("in func persist, me:", rf.me, "test decode, term:", term, "votedFor", votedFor, "lastApplied:", lastApplied, "afterSnaoshotIndex:", afterSnapshotIndex, "logs:", logs)
	}
	//fmt.Println("in func persist, test decode end")*/
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	//fmt.Println("in func readPersist, rf:", rf.me, "Term:", rf.GetCurrentTerm(), "data:", data)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []LogEntry
	var lastApplied int32
	var afterSnapshotIndex int32
	var lastIncludeTerm int32

	err := d.Decode(&term)
	if err != nil {
		fmt.Println("in func readPersist, decode term error, ", err)
	}

	err = d.Decode(&votedFor)
	if err != nil {
		fmt.Println("in func readPersist, decode votedFor error, ", err)
	}

	err = d.Decode(&logs)
	if err != nil {
		fmt.Println("in func readPersist, decode logs error, ", err)
	}

	err = d.Decode(&lastApplied)
	if err != nil {
		fmt.Println("in func readPersist, decode lastApplied error, ", err)
	}

	err = d.Decode(&afterSnapshotIndex)
	if err != nil {
		fmt.Println("in func readPersist, decode afterSnapshotIndex error, ", err)
	}

	err = d.Decode(&lastIncludeTerm)
	if err != nil {
		fmt.Println("in func readPersist, decode lastIncludeTerm error, ", err)
	}

	if err != nil {
		fmt.Println("in func readPersist, me:", rf.me, "term:", term, "votedFor", votedFor, "lastApplied:", lastApplied, "afterSnapshotIndex:", afterSnapshotIndex,
			"lastIncludeInclude:", lastIncludeTerm,
			"logs:", logs, "decode error")
		return
	} else {
		//fmt.Println("in func readPersist, before voteMu lock")
		rf.voteMu.Lock()
		//fmt.Println("in func readPersist, after voteMu lock")
		fmt.Println("in func readPersist, me:", rf.me, "term:", term, "votedFor", votedFor, "lastApplied:", lastApplied, "afterSnapshotIndex:", afterSnapshotIndex, "lastIncludeTerm:", lastIncludeTerm,
			"logs:", logs)
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastApplied = int32(lastApplied)
		rf.afterSnapshotIndex = afterSnapshotIndex
		rf.lastIncludeTerm = lastIncludeTerm
		//fmt.Println("in func readPersist, before voteMu unlock")
		rf.voteMu.Unlock()
		//fmt.Println("in func readPersist, after voteMu unlock")
		//rf.SetVoteState(term, votedFor)
		//rf.AppendLogs(0, logs)
		//fmt.Println("in func readPersist, after decode, term:", rf.GetCurrentTerm(), "votedFor:", rf.GetVotedFor(), "logs:", rf.GetLogs(0))
	}
	/*if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&afterSnapshotIndex) != nil {
		fmt.Println("in func readPersist, decode error")
		fmt.Println("in func readPersist, me:", rf.me, "term:", term, "votedFor", votedFor, "lastApplied:", lastApplied, "afterSnapshotIndex:", afterSnapshotIndex, "logs:", logs, "decode error")
		return
	}*/
}

func (rf *Raft) GetPersister() *Persister {
	return rf.persister
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Candidate int
	LastLogIndex int32
	LastLogTerm int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int32
	PrevLogTerm int32
	Entries []LogEntry
	LeaderCommit int32

}

type AppendEntriesReply struct {
	Term int
	Succ bool
	CommitIndex int32
	ConflictIndex int32
	ConflictTerm int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {    //raft节点rf接收到投票请求后，如何去处理这个请求，并且把内容写到RequestVorereply
	// Your code here (2A, 2B).

	//fmt.Println("in func RequestVote: begin")

	rf.UpdateTerm(args.Term)

	currentTerm := rf.GetCurrentTerm()
	reply.Term = currentTerm

	if currentTerm > args.Term /*|| (currentTerm == args.Term && state == LEADER)*/ { //选举人任期更早，淘汰
		//fmt.Println("in func RequestVote, rf:", rf.me, "candidate:", args.Candidate, "rf.Term > args.Term")
		reply.VoteGranted = false
		return
	}

	rf.UpdateLastRecvTime()

	if rf.isLogNew(args) {
		//fmt.Println("in func RequestVote, rf:", rf.me, "candidate:", args.Candidate, "rf.logs new")
		reply.VoteGranted = false
		return
	}

	votedFor := rf.GetVotedFor()
	if votedFor == -1 || votedFor == args.Candidate {
		//fmt.Println("in func RequestVote, rf:", drf.me, "candidate:", args.Candidate, "vote success")
		rf.SetVotedFor(args.Candidate)
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Println("in func AppendEntries, begin")
	if (args == nil) {
		//fmt.Println("in func AppendEntries, begin, rf:", rf.me, "rf.term:", rf.GetCurrentTerm(), "state:", rf.GetCertainState(), "args is nil")
	}
	/*fmt.Println("in func AppendEntries, begin, rf:", rf.me, "rf.term:", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
		"leader:", args.LeaderId, "leader.Term", args.Term, "rf.lastLogIndex:", rf.GetLastLogIndex(), "args.lastLogIndex:", args.PrevLogIndex, "rf.afterSnaoShotIndex:", rf.GetAfterSnapshotIndex(),
		"logLen:", rf.GetLogsLength())*/
	rf.UpdateTerm(args.Term)
	currentTerm, _ := rf.GetVoteState()
	reply.Term = currentTerm

	if (currentTerm > args.Term) {  //该结点收到leader的heartbeat，但是自己的任期更大，拒绝
		//fmt.Println("in func AppendEntries: rf:", rf.me, "rf.term:", rf.GetCurrentTerm(), "rf.Term > args.Term")
		reply.Succ = false
		return
	}
	rf.UpdateLastRecvTime()
	atomic.StoreUint32(&rf.state, FOLLOWER)
	//fmt.Println("in func AppendEntries, rf:", rf.me, "term:", rf.GetCurrentTerm(), "state:", rf.GetCertainState())
	reply.Succ = true

	logLen := int32(rf.GetLogsLength())
	//lastLogTerm := rf.GetLastLogTerm()
	commitIndex := rf.GetCommitIndex()
	afterSnapshotIndex := rf.GetAfterSnapshotIndex()
	lastApplied := rf.GetLastApplied()

	termOfPrevLogIndexInRf := rf.GetSingleEntry(args.PrevLogIndex).Term
	//fmt.Println("in func AppendEntries, rf:", rf.me, "term:", rf.GetCurrentTerm(), "termOfPrevLogIndexInRf:", termOfPrevLogIndexInRf)

	if args.PrevLogIndex > logLen + afterSnapshotIndex /*|| args.LeaderCommit > logLen - 1*/ {
		//fmt.Println("in func AppendEntries, me:", rf.me, "111, args.PrevLogIndex:", args.PrevLogIndex, "logLen:", logLen, "afterSnapshotIndex:", afterSnapshotIndex)
		/*fmt.Println("in func AppendEntries,  rf:", rf.me, "rf.term:", rf.GetCurrentTerm(),
			"args.PervLogIndex > logLen, args.PrevLogIndex:", args.PrevLogIndex, "logLen:", logLen)*/
		reply.Succ = false
		reply.CommitIndex = commitIndex
		reply.Term = args.Term
		reply.ConflictIndex = afterSnapshotIndex
		//fmt.Println("in func AppendEntries, me:", rf.me, "111, args.PrevLogIndex:", args.PrevLogIndex, "logLen:", logLen, "afterSnapshotIndex:", afterSnapshotIndex,
		//	"conflictIndex:", reply.ConflictIndex)
	} else if args.PrevLogIndex >= afterSnapshotIndex && args.PrevLogTerm != int32(termOfPrevLogIndexInRf)/*lastLogTerm*/ {
		/*fmt.Println("in func AppendEntries, rf:", rf.me, "rf.term:", rf.GetCurrentTerm(),
			"args.PrevLogIndex >= 0 && args.PrevLogTerm != termOfPrevLogIndexInRf, " +
			"args.PrevLogIndex:", args.PrevLogIndex,
			"PrevLogTerm:", args.PrevLogTerm, "termOfPrevLogIndexInRf:", termOfPrevLogIndexInRf,
			"rf.logs", rf.GetLogs(0), "args.logs", args.Entries)*/
		//fmt.Println("in func AppendEntries, me:", rf.me, "222")
		reply.Succ = false
		reply.CommitIndex = commitIndex
		reply.Term = args.Term
		reply.ConflictTerm = termOfPrevLogIndexInRf
		i := args.PrevLogIndex
		//for ; i > 0 && rf.GetSingleEntry(i - rf.GetAfterSnapshotIndex).Term == termOfPrevLogIndexInRf; i--
		for ; i > afterSnapshotIndex && rf.GetSingleEntry(i).Term == termOfPrevLogIndexInRf; i-- {

		}
		reply.ConflictIndex = i
	} else if args.PrevLogIndex >= afterSnapshotIndex - 1 {
		/*fmt.Println("in func AppendEntries, rf:", rf.me, "rf.term:", rf.GetCurrentTerm(), "state", rf.GetCertainState(),
			"match",
			"args.PrevLogIndex:", args.PrevLogIndex,
			"PrevLogTerm:", args.PrevLogTerm, "termOfPrevLogIndexInRf:", termOfPrevLogIndexInRf,
			"rf.logs", rf.GetLogs(0), "args.logs", args.Entries)*/
		//fmt.Println("in func AppendEntries, me:", rf.me, "333")
		reply.Succ = true
		reply.CommitIndex = args.LeaderCommit
		reply.Term = args.Term

		rf.AppendLogs(args.PrevLogIndex + 1, args.Entries)
		rf.SetCommitIndex(args.LeaderCommit)
		if lastApplied < rf.GetCommitIndex() {
			go rf.commitLogs()  //会不会提交到一半被覆盖？
		}
	} else { // 有时候会出现args.PrevLogIndex远远小于rf.afterSnapshotIndex的情况
		fmt.Println("in func AppendEntries, me:", rf.me, "leader:", args.LeaderId, "PrevLogIndex < afterSnapshotIndex, PrevLogIndex:", args.PrevLogIndex, "afterSnapshotIndex:", rf.GetAfterSnapshotIndex())
		reply.Succ = false
		reply.Term = args.Term
	}
}


type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludeIndex int32
	LastIncludeTerm int32
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
	Succ bool
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	fmt.Println("in func InstallSnapshot, me:", rf.me, "leader:", args.LeaderId, "args.term:", args.Term, "rf.term", rf.currentTerm,
		"args.LastIndcludeIndex:", args.LastIncludeIndex, "rf.index:", rf.GetLastLogIndex())
	rf.UpdateTerm(args.Term)

	currentTerm, _ := rf.GetVoteState()
	reply.Term = currentTerm

	if (currentTerm > args.Term) {  //该结点收到leader的heartbeat，但是自己的任期更大，拒绝
		//fmt.Println("in func AppendEntries: rf:", rf.me, "rf.term:", rf.GetCurrentTerm(), "rf.Term > args.Term")
		reply.Succ = false
		return
	}

	//rf.UpdateLastRecvTime()
	//atomic.StoreUint32(&rf.state, FOLLOWER)
	//fmt.Println("in func AppendEntries, rf:", rf.me, "term:", rf.GetCurrentTerm(), "state:", rf.GetCertainState())
	rf.UpdateLastRecvTime()
	reply.Succ = true
	//rf.voteMu.Lock()
	//fmt.Println("in func InstallSnapshot, voteMu Lock")

	rf.voteMu.RLock()
	lastLogIndex := int32(len(rf.logs)) - 1 + rf.afterSnapshotIndex
	lastApplied := rf.GetLastApplied()
	lastIncludeTerm := rf.lastIncludeTerm
	afterSnapshotIndex := rf.afterSnapshotIndex
	rf.voteMu.RUnlock()


	if args.LastIncludeIndex > lastLogIndex || args.LastIncludeTerm > lastIncludeTerm { // 快照全部替换
		//fmt.Println("in func InstallSnapshot, all should change, begin")
		minIndex := afterSnapshotIndex
		if afterSnapshotIndex > lastApplied + 1 {
			minIndex = lastApplied + 1
		}
		rf.persister.SaveStateAndSnapshot(rf.RaftStateToSnapshot(minIndex), args.Data) // lastApplied may be -1
		//rf.persister = rf.persister.Copy()
		//rf.SetAfterSnapshotIndex(args.LastIncludeIndex + 1)
		//rf.logs = make([]LogEntry, 0)
		//rf.voteMu.Unlock()
		//fmt.Println("in func InstallSnapshot, all should change, done")
	} else if args.LastIncludeIndex > afterSnapshotIndex { // 替换掉前面的日志
		//fmt.Println("in func InstallSnapshot, pre should change, begin")
		minIndex := afterSnapshotIndex
		if afterSnapshotIndex > lastApplied + 1 {
			minIndex = lastApplied + 1
		}

		rf.logs = rf.logs[minIndex - afterSnapshotIndex:]
		//rf.voteMu.Unlock()

		//fmt.Println("in func InstallSnapshot, SetAfterSnapshotIndex")
		rf.SetAfterSnapshotIndex(minIndex)

		rf.persist()

		rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, args.Data)
		//rf.persister = rf.persister.Copy()
		//fmt.Println("in func InstallSnapshot, pre should change, done")
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args* InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//defer fmt.Println("in func sendInstallSnapshot: sender:", rf.me, "server:", server, "end")
	//fmt.Println("in func sendInstallSnapshot: sender:", rf.me, "server:", server, "begin")
	okCh := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		okCh <- ok
	}()
	ok := false
	select{
	case ok = <- okCh:
		return ok
	case <- time.After(50 * time.Millisecond):
		return false
	}
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {    //发送请求给server，让server来选自己
	//fmt.Println("in func sendRequestVote: sender:", rf.me, "voter:", server)
	okCh := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		okCh <- ok
	}()
	ok := false
	select{
	case ok = <- okCh:
		return ok
	case <- time.After(50 * time.Millisecond):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args* AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//defer fmt.Println("in func sendAppendEntries: sender:", rf.me, "server:", server, "end")
	//fmt.Println("in func sendAppendEntries: sender:", rf.me, "server:", server, "begin")
	okCh := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		okCh <- ok
	}()
	ok := false
	select{
	case ok = <- okCh:
		return ok
	case <- time.After(50 * time.Millisecond):
		return false
	}
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).

	term := rf.GetCurrentTerm()
	state := rf.GetCertainState()
	//index := rf.GetLogsLength()
	//fmt.Println("in func Start, cmd:", command, "rf:", rf.me, "index:", index)
	//fmt.Println("in func Start, me:", rf.me, "state:", state)
	if state != LEADER {
		//fmt.Println("in func Start, me:", rf.me, "false")
		return -1, term, false
	}

	//cmd, _ := Encode(command)
	//fmt.Println("in func Start, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func Start, after voteMu lock")
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	logEntries := []LogEntry{logEntry}
	//fmt.Println("term:", rf.currentTerm, "cmd:", cmd)

	//rf.AppendLogs(int32(index), logEntries)  //同时拿到同一个index，相互覆盖
	rf.logs = append(rf.logs, logEntries...)
	index := int32(len(rf.logs)) - 1 + rf.afterSnapshotIndex
	//fmt.Println("in func Start, before voteMu unlock")
	rf.voteMu.Unlock()
	//fmt.Println("in func Start, after voteMu unlock")
	rf.cmdCh <- index

	//fmt.Println("in func Start, rf:", rf.me, "term:", rf.currentTerm, "index:", index, "cmd", command)
	//fmt.Println("in func Start, me:", rf.me, "true")
	return int(index) + 1, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) LeaderElection() {
	//fmt.Println("in func LeaderElection: rf:", rf.me, "term:", rf.GetCertainState())
	atomic.StoreUint32(&rf.state, CANDIDATE)
	var currentTerm int
	for {
		state := rf.GetCertainState()
		if (rf.killed() || state != CANDIDATE) {
			//fmt.Println("in func LeaderElection, rf:", rf.me, "rf.Term:", rf.GetCurrentTerm(), "commitIndex:",
			//	rf.GetCommitIndex(), "length of logs:", rf.GetLogsLength(), "not candidate anymore")
			return
		}

		currentTerm, _ = rf.TurnToCandidate()
		fmt.Println("in func LeaderElection, rf:", rf.me, "rf.Term:", rf.GetCurrentTerm(), "commitIndex:",
			rf.GetCommitIndex(), "length of logs:", rf.GetLogsLength(), "begin election")
		itself := rf.GetItself()

		replyCount := make(chan int, len(rf.peers))
		rand.Seed(time.Now().UnixNano())
		timeout := time.Duration(time.Duration(
			rand.Intn(500)+500) * time.Millisecond)
		go func() {
			<-time.After(timeout)
			replyCount <- -1
		}()

		for i := 0; i < len(rf.peers); i++ {  //遍历所有节点
			if i == itself {
				continue
			}
			go func(voter int) {  //拉票请求，向voter节点发请求

				args := RequestVoteArgs{
					Term:         currentTerm,
					Candidate:    itself,
					LastLogIndex: rf.GetLastLogIndex(),
					LastLogTerm:  rf.GetLastLogTerm(),
				}
				reply := RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}
				ok := false  //向voter节点发送请求
				okCnt := 0
				for !ok {
					ok = rf.sendRequestVote(voter, &args, &reply)
					okCnt++
					//fmt.Println("in func leaderElection's goroutine, rf:", rf.me, "term", rf.GetCurrentTerm(), "ok:", ok, "okCnt:", okCnt)
					time.Sleep(10 * time.Millisecond)
				}

				/*if term, _ := rf.GetVoteState(); term != args.Term {    //在期间任期发生变化，本次选举失效
					fmt.Println("in func LeaderElection's goroutine: rf is", rf.me, "term is", reply.Term, "term change")
					return
				}*/
				rf.UpdateTerm(reply.Term)

				if reply.VoteGranted {
					replyCount <- voter
				}

				//fmt.Println("in func LeaderElection's goroutine: candidate:", rf.me, "state:", rf.GetCertainState(),
				//	"voter:", voter, "term:", reply.Term, "vote or not:", reply.VoteGranted)

			}(i)
		}

		votedCnt := 1
		majority := len(rf.peers) / 2 + 1  //设置多数值
		for {
			v := <- replyCount
			if v != -1 {
				votedCnt++
			} else {
				break
			}
			if votedCnt >= majority {
				break
			}
		}

		if votedCnt >= majority/* && votedCnt > 1 */{
			break
		}
		fmt.Println("in func LeaderElection, rf:", rf.me, "rf.Term:", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
			"commitIndex:", rf.GetCommitIndex(), "length of logs:", rf.GetLogsLength(), "not become leader, sleep for a while")
		rand.Seed(time.Now().UnixNano())
		timeout = time.Duration(time.Duration(
			rand.Intn(500)+500) * time.Millisecond)
		time.Sleep(timeout)
	}
	//
	if currentTerm == rf.GetCurrentTerm() && rf.GetCertainState() == CANDIDATE {
		fmt.Println("in func leaderElection, rf:", rf.me, "term:", rf.GetCurrentTerm(), "commitIndex:",
			rf.GetCommitIndex(), "length of logs:", rf.GetLogsLength(), "win election")
		for len(rf.cmdCh) > 0 {
			<- rf.cmdCh
		}
		rf.SetCertainState(LEADER)
		rf.TurnToLeader()
		rf.heartbeat2()
	}
}


func (rf *Raft) heartbeat2() {
	replyCh := make(chan int32, 1024)
	peersChs := make([]chan int32, len(rf.peers), 1024)
	for i := 0; i < len(rf.peers); i++ {
		peersChs[i] = make(chan int32, 1024)
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
			//	"receiver:", server, "begin go func")
			term := rf.GetCurrentTerm()
			for {
				if rf.killed() || rf.GetCertainState() != LEADER {
					break
				}
				//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
				//	"receiver:", server, "begin one loop")
				select {
				case <- time.After(HEARTBEAT_INTERVAL / 2):
					//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
					//	"receiver:", server, ", before args, after heartbeat")

					ok := false
					okCnt := 0
					for !ok /*&& okCnt < 10*/ && rf.GetCertainState() == LEADER {
						//fmt.Println("in func heartbeat2's goroutine, case heartbeat, sender:", rf.me, "receiver:", server,
						//	"nextIndex:", rf.GetNextIndex(server), "afterSnapshotIndex:", rf.afterSnapshotIndex)
						afterSnapshotIndex := rf.GetAfterSnapshotIndex()
						if rf.GetNextIndex(server) < afterSnapshotIndex {
							//fmt.Println("in func heartbeat2's goroutine, case heartbeat, installSnapshot, sender:", rf.me, "receiver:", server)
							args := InstallSnapshotArgs{
								Term:             term,
								LeaderId:         rf.GetItself(),
								LastIncludeIndex: afterSnapshotIndex - 1,
								LastIncludeTerm:  rf.GetLastIncludeTerm(),
								Offset:           0,
								Data:             rf.persister.ReadSnapshot(),
								Done:             false,
							}
							//fmt.Println("in func heartbeat2's goroutine, args.data:", args.Data)
							var reply InstallSnapshotReply
							ok := rf.sendInstallSnapshot(server, &args, &reply)
							okCnt = 0
							if !ok && rf.GetCertainState() == LEADER && !reply.Succ {
								ok = rf.sendInstallSnapshot(server, &args, &reply)
								okCnt++
								//fmt.Println("in func heartbeat2's goroutine, case heartbeat, installsnapshot, okCnt:", okCnt,
								//	"ok:", ok, "reply.Succ:", reply.Succ)
								time.Sleep(10 * time.Millisecond)
							}
							rf.UpdateTerm(reply.Term)
							if rf.killed() || rf.GetCertainState() != LEADER || rf.GetCurrentTerm() != term {
								//fmt.Println("in func heartbeat2's goroutine, case heartbeat, installsnapshot, change")
								break
							}
							if ok && reply.Succ {
								//fmt.Println("in func heartbeat2's goroutine, case heartbeat, installSnapshot, sender:", rf.me, "receiver:", server, "succ")
								rf.SetNextIndex(server, afterSnapshotIndex)
								//break
							}

						} else {
							//fmt.Println("in func heartbeat2's goroutine, case heartbeat, AppendEntries, sender:", rf.me, "receiver:", server)
							args := AppendEntriesArgs{
								Term:         term,
								LeaderId:     rf.GetItself(),
								PrevLogIndex: rf.GetPrevLogIndex(server),
								PrevLogTerm:  rf.GetPrevLogTerm(server),
								Entries:      rf.GetLogsBehindNextIndex(server),
								LeaderCommit: rf.GetCommitIndex(),
							}
							//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
							//	"receiver:", server, "after args, after heartbeat")
							reply := AppendEntriesReply{
								Term:          0,
								Succ:          false,
								CommitIndex:   0,
								ConflictIndex: 0,
								ConflictTerm:  0,
							}
							ok = rf.sendAppendEntries(server, &args, &reply)
							okCnt++
							if !(!ok /*&& okCnt < 10*/ && rf.GetCertainState() == LEADER) {
								rf.UpdateTerm(reply.Term)
								if rf.killed() || rf.GetCertainState() != LEADER || rf.GetCurrentTerm() != term {
									//fmt.Println("in func heartbeat2, term change")
									break
								}
								if ok && !reply.Succ {
									//nextIndex1 := rf.GetNextIndex(server)
									//rf.SetNextIndex(server, rf.GetNextIndex(server) - 1)
									//fmt.Println("in func heartbeat2, me:", rf.me, "conflictIndex:", reply.ConflictIndex)
									rf.SetNextIndex(server, reply.ConflictIndex)
									//nextIndex2 := rf.GetNextIndex(server)
									//fmt.Println("in func heartbeat2's goroutine, rf:", rf.me, "term:", term, "server", server,
									//	"nextIndex1:", nextIndex1, "nextIndex2:", nextIndex2)
								}
								break
							}
							//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
							//	"receiver:", server, "ok:", ok, "okCnt:", okCnt, "heartbeat")
							time.Sleep(10 * time.Millisecond)
						}
					}

				case index := <-peersChs[server]:
					for {
						//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
						//	"receiver:", server, "index:", index)
						//fmt.Println("in func heartbeat2's goroutine, case indexCome, sender:", rf.me, "receiver:", server,
						//	"nextIndex:", rf.GetNextIndex(server), "afterSnapshotIndex:", rf.afterSnapshotIndex)
						if rf.GetNextIndex(server) < rf.GetAfterSnapshotIndex() {
							//fmt.Println("in func heartbeat2's goroutine, case indexCome, installSnapshot, sender:", rf.me, "receiver:", server)
							args := InstallSnapshotArgs{
								Term:             term,
								LeaderId:         rf.GetItself(),
								LastIncludeIndex: rf.GetAfterSnapshotIndex() - 1,
								LastIncludeTerm:  rf.GetLastIncludeTerm(),
								Offset:           0,
								Data:             rf.persister.ReadSnapshot(),
								Done:             false,
							}
							//fmt.Println("in func heartbeat2's goroutine, args.data:", args.Data)
							var reply InstallSnapshotReply
							ok := rf.sendInstallSnapshot(server, &args, &reply)
							okCnt := 0
							if !ok && rf.GetCertainState() == LEADER && rf.GetCurrentTerm() == term && !reply.Succ {
								ok = rf.sendInstallSnapshot(server, &args, &reply)
								okCnt++
								//fmt.Println("in func heartbeat2's goroutine, case indexCome, installsnapshot, sender:",
								//	rf.me, "receiver:", server, "okCnt:", okCnt, "ok:", ok, "reply.Succ:", reply.Succ)
								time.Sleep(10 * time.Millisecond)
							}
							rf.UpdateTerm(reply.Term)
							if rf.killed() || rf.GetCertainState() != LEADER || rf.GetCurrentTerm() != term {
								//fmt.Println("in func heartbeat2's goroutine, case indexCome, installsnapshot, change")
								break
							}
							if ok && reply.Succ {
								rf.SetNextIndex(server, rf.GetAfterSnapshotIndex())
								/*if rf.GetAfterSnapshotIndex() >= index {
									replyCh <- index
									break
								}*/
								replyCh <- index
								break
							}
						} else {
							//fmt.Println("in func heartbeat2's goroutine, case indexCome, AppendEntries, sender:", rf.me, "receiver:", server)
							args := AppendEntriesArgs{
								Term:         term,
								LeaderId:     rf.GetItself(),
								PrevLogIndex: rf.GetPrevLogIndex(server),
								PrevLogTerm:  rf.GetPrevLogTerm(server),
								Entries:      rf.GetLogsBehindNextIndex(server),
								LeaderCommit: rf.GetCommitIndex(),
							}
							//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
							//"receiver:", server, ", after args, index come")
							reply := AppendEntriesReply{}

							ok := false
							for !ok /*&& okCnt < 10*/  && rf.GetCertainState() == LEADER && rf.GetCurrentTerm() == term {
								ok = rf.sendAppendEntries(server, &args, &reply)
								time.Sleep(10 * time.Millisecond)
							}
							rf.UpdateTerm(reply.Term)
							if rf.killed() || rf.GetCertainState() != LEADER || rf.GetCurrentTerm() != term {
								break
							}
							if ok && reply.Succ {
								rf.SetNextIndex(server, rf.GetNextIndex(server) + int32(len(args.Entries)))
								replyCh <- index
								break
								//break
							} else if ok && !reply.Succ {
								//nextIndex1 := rf.GetNextIndex(server)
								//rf.SetNextIndex(server, rf.GetNextIndex(server) - 1)
								//fmt.Println("in func heartbeat2's goroutine, case indexCome, AppendEntries, sender:", rf.me, "receiver:", server, "conflictIndex:", reply.ConflictIndex)
								rf.SetNextIndex(server, reply.ConflictIndex)
								//fmt.Println("in func heartbeat2's goroutine, sender:", rf.me, "term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
								//	"receiver:", server, "sendAppendEntries change nextIndex")
								//nextIndex2 := rf.GetNextIndex(server)
								//fmt.Println("in func heartbeat2's goroutine, rf:", rf.me, "term:", term, "server", server,
								//	"nextIndex1:", nextIndex1, "nextIndex2:", nextIndex2)
							}
						}
					}
				}
			}
		}(i)
	}


	go func() {
		for {
			if rf.killed() || rf.GetCertainState() != LEADER {
				break
			}
			index := <-rf.cmdCh
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				peersChs[i] <- index
			}

			majority := len(rf.peers) / 2 + 1
			replyCnt := 1
			for {
				v := <- replyCh
				if v < index {
					continue
				}
				replyCnt++
				if replyCnt >= majority {
					break
				}
			}
			if replyCnt >= majority {
				rf.SetCommitIndex(index)
				if rf.GetLastApplied() < rf.GetCommitIndex() {
					go rf.commitLogs()
				}
			}
		}
	}()
}


func (rf *Raft) commitLogs() {
	commitIndex := rf.GetCommitIndex()
	lastLogIndex := rf.GetLastLogIndex()
	lastApplied := rf.GetLastApplied()
	/*term := rf.GetCurrentTerm()
	logs := rf.GetLogs(0)
	state := rf.GetCertainState()
	//fmt.Println("in func commitLogs, rf:", rf.me, "state", state, "term", term, "commitIndex:", commitIndex,
		"lastLogIndex", lastLogIndex, "lastApplied:", rf.lastApplied, "logs:", logs)*/

	if (commitIndex > lastLogIndex) {
		rf.SetCommitIndex(lastLogIndex)
		commitIndex = lastLogIndex
	}

	for i := lastApplied + 1; i <= commitIndex; i++ {
		//fmt.Println("in func commitLogs, rf:", rf.me, "commitIndex:", commitIndex, "commandIndex:", i, "command", rf.logs[i].Command)
		//fmt.Println("in func commitLogs, before voteMu lock")
		rf.voteMu.Lock()
		//fmt.Println("in func commitLogs, after voteMu lock")
		if i - rf.afterSnapshotIndex < 0 || int(i - rf.afterSnapshotIndex) >len(rf.logs) {
			fmt.Println("in func commitLogs, i:", i, "afterSnapshotIndex:", rf.afterSnapshotIndex)
		}
		rf.applyCh <- ApplyMsg{
			CommandIndex: int(i + 1),
			Command: rf.logs[i - rf.afterSnapshotIndex].Command,
			CommandValid: true,
		}
		//rf.persist()
		rf.lastApplied = i
		//fmt.Println("in func commitLogs, me:", rf.me, "lastApplied:", rf.lastApplied)
		rf.persist()
		//fmt.Println("in func commitLogs, before voteMu unlock")
		rf.voteMu.Unlock()
		//fmt.Println("in func commitLogs, after voteMu unlock")
		//rf.SetLastApplied(i)
	}

	//rf.SetLastApplied(commitIndex)
	//rf.SetCommitIndex(commitIndex + 1)
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("in func Make, rf:", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//rf.SetVoteState(0, -1)
	if len(persister.ReadRaftState()) > 0 {
		rf.readPersist(persister.ReadRaftState())
	} else {
		//fmt.Println("in func Make, raftState is empty")
		//fmt.Println("in func Make, before voteMu lock")
		rf.voteMu.Lock()
		//fmt.Println("in func Make, after voteMu lock")
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.logs = make([]LogEntry, 0)
		rf.lastApplied = -1
		rf.afterSnapshotIndex = 0
		rf.lastIncludeTerm = -1
		rf.persist()
		//fmt.Println("in func Make, before voteMu unlock")
		rf.voteMu.Unlock()
		//fmt.Println("in func Make, after voteMu unlock")
	}
	rf.SetCertainState(FOLLOWER)

	rf.SetCommitIndex(-1)
	//rf.SetLastApplied(-1)

	rf.nextIndexMu.Lock()
	rf.nextIndex = make([]int32, len(rf.peers))
	rf.nextIndexMu.Unlock()

	rf.matchIndexMu.Lock()
	rf.matchIndex = make([]int32, len(rf.peers))
	rf.matchIndexMu.Unlock()

	/*rf.logsMu.Lock()
	rf.logs = make([]LogEntry, 0)
	rf.logsMu.Unlock()*/

	rf.applyCh = applyCh

	rf.heartbeatInterval = HEARTBEAT_INTERVAL  //错开选举超时的时间，否则大家都会同时超时，同时发起选举，只会投自己，永远宣不出leader
	rf.electionInterval = time.Duration(time.Duration(rand.Intn(500)+500) * time.Millisecond)

	//rf.electionInterval = time.Duration(time.Duration(1000) * time.Millisecond)
	rf.cmdCh = make(chan int32, 1024)

	//rf.SetAfterSnapshotIndex(0)

	go func() {
		for {
			if (rf.killed()) {
				break
			}

			time.Sleep(rf.electionInterval)

			state := rf.GetCertainState()
			if state != FOLLOWER {
				continue
			}

			now := time.Now().UnixNano()
			prev := atomic.LoadInt64(&rf.lastRecvTime)
			if time.Duration(now - prev) * time.Nanosecond >= rf.electionInterval {
				//fmt.Println("in func Make's goroutine, rf:", rf.me, "term:", rf.GetCurrentTerm(), "launch Election")
				rf.LeaderElection()
			}
		}
	}()


	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())    //未完成

	return rf
}

func (rf* Raft) GetVoteState() (int, int) {
	//fmt.Println("in func GetVoteState, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetVoteState, after voteMu rlock")
	//defer fmt.Println("in func GetVoteState, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetVoteState, before voteMu runlock")
	return rf.currentTerm, rf.votedFor
}

func (rf *Raft) SetVoteState(term int, votedFor int) {
	//fmt.Println("in func SetVoteState, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func SetVoteState, after voteMu lock")
	//defer fmt.Println("in func SetVoteState, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func SetVoteState, before voteMu unlock")

	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = votedFor
		//fmt.Println("in func SetVoteState, persist")
		rf.persist()
	}

	//rf.persist()
}

func (rf* Raft) GetCurrentTerm() int {

	//fmt.Println("in func GetCurrentTerm, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetCurrentTerm, after voteMu rlock")
	//defer fmt.Println("in func GetCurrentTerm, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetCurrentTerm, before voteMu runlock")
	return rf.currentTerm
}

func (rf* Raft) GetVotedFor() int {
	//fmt.Println("in func GetVotedFor, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetVotedFor, after voteMu rlock")
	//defer fmt.Println("in func GetVotedFor, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetVotedFor, before voteMu runlock")
	return rf.votedFor
}

func (rf *Raft) SetVotedFor(votedFor int) {
	//fmt.Println("in func SetVotedFor, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func SetVotedFor, after voteMu lock")
	//defer fmt.Println("in func SetVotedFor, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func SetVotedFor, before voteMu unlock")
	rf.votedFor = votedFor

	//fmt.Println("in func SetVotedFor, persist")
	rf.persist()
}

func (rf *Raft) TurnToCandidate() (int, int) {
	//fmt.Println("in func TurnToCandidate, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func TurnToCandidate, after voteMu lock")
	//defer fmt.Println("in func TurnToCandidate, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func TurnToCandidate, before voteMu unlock")
	rf.currentTerm++;
	rf.votedFor = rf.me

	//fmt.Println("in func TurnToCandidate, persist")
	rf.persist()
	return rf.currentTerm, rf.votedFor
}

func (rf *Raft) TurnToLeader() () {
	rf.nextIndexMu.Lock()
	rf.voteMu.RLock()
	logsLen := len(rf.logs)
	afterSnapshotIndex := rf.afterSnapshotIndex
	for server := 0; server < len(rf.peers); server++ {
		rf.nextIndex[server] = int32(logsLen) + afterSnapshotIndex
	}
	rf.voteMu.RUnlock()
	rf.nextIndexMu.Unlock()

	rf.matchIndexMu.Lock()
	for server := 0; server < len(rf.peers); server++ {
		rf.matchIndex[server] = 0
	}
	rf.matchIndexMu.Unlock()
}

func (rf *Raft) GetCertainState() uint32 {
	return atomic.LoadUint32(&rf.state)
}

func (rf *Raft) SetCertainState(state uint32) {
	atomic.StoreUint32(&rf.state, state)
}

func (rf* Raft) GetItself() int {
	return rf.me
}

func (rf* Raft) UpdateTerm(term int) {
	//fmt.Println("in func UpdateTerm, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func UpdateTerm, after voteMu lock")
	//defer fmt.Println("in func UpdateTerm, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func UpdateTerm, before voteMu unlock")

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1;
		atomic.StoreUint32(&rf.state, FOLLOWER)
		//fmt.Println("in func UpdateTerm, persist")
		rf.persist()
	}

	//fmt.Println("in func UpdateTerm")
	//rf.persist()
}

func (rf *Raft) UpdateLastRecvTime() {
	now := time.Now().UnixNano()
	atomic.StoreInt64(&rf.lastRecvTime, now)
}

func (rf *Raft) isLogNew(args *RequestVoteArgs) bool {
	//currentTerm := rf.GetCurrentTerm()
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.GetLastLogTerm()
	/*fmt.Println("in func isLogNew, rf:", rf.me, "args.Candidate:", args.Candidate,  "args.Term:", args.Term,
		"lastLogTerm:", lastLogTerm, "args.LastLogTerm:", args.LastLogTerm,
		"lastLogIndex:", lastLogIndex, "args.lastLogIndex:", args.LastLogIndex)*/
	return lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex)
}

func (rf* Raft) GetLogsLength() int {
	//fmt.Println("in func GetLogsLength, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogsLength, after voteMu rlock")
	//defer fmt.Println("in func GetLogsLength, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogsLength, before voteMu runlock")

	return len(rf.logs)
}

func (rf* Raft) GetLastLogIndex() int32 {
	//rf.logsMu.RLock()
	//defer rf.logsMu.RUnlock()

	//return int32(rf.GetLogsLength() - 1)

	//fmt.Println("in func GetLastLogIndex, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLastLogIndex, after voteMu rlock")
	//defer fmt.Println("in func GetLastLogIndex, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLastLogIndex, before voteMu runlock")

	return int32(len(rf.logs)) - 1 + rf.afterSnapshotIndex
}

func (rf* Raft) GetPrevLogIndex(server int) int32 {
	return rf.GetNextIndex(server) - 1
}

func (rf* Raft) GetLastLogTerm() int32 {
	//fmt.Println("in func GetLastLogTerm, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLastLogTerm, after voteMu rlock")
	//defer fmt.Println("in func GetLastLogTerm, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLastLogTerm, before voteMu runlock")

	index := int32(len(rf.logs)) - 1 + rf.afterSnapshotIndex
	if index < rf.afterSnapshotIndex {
		return -1
	}
	return int32(rf.logs[index - rf.afterSnapshotIndex].Term)
}

func (rf* Raft) GetPrevLogTerm(server int) int32 {
	//fmt.Println("in func GetPrevLogTerm, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetPrevLogTerm, after voteMu rlock")
	//defer fmt.Println("in func GetPrevLogTerm, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetPrevLogTerm, before voteMu runlock")

	index := rf.GetPrevLogIndex(server)
	if index < rf.afterSnapshotIndex || int(index) >= len(rf.logs) + int(rf.afterSnapshotIndex) {
		return -1
	}
	return int32(rf.logs[index - rf.afterSnapshotIndex].Term)
}

func (rf* Raft) SetCommitIndex(index int32) {
	atomic.StoreInt32(&rf.commitIndex, index)
}

func (rf* Raft) GetLastApplied() int32 {
	return atomic.LoadInt32(&rf.lastApplied)
}

func (rf* Raft) SetLastApplied(lastApplied int32) {
	//rf.voteMu.Lock()
	//defer rf.voteMu.Unlock()

	atomic.StoreInt32(&rf.lastApplied, lastApplied)
	//rf.persist()
}

func (rf *Raft) GetNextIndex(server int) int32 {
	rf.nextIndexMu.RLock()
	defer rf.nextIndexMu.RUnlock()

	
	return rf.nextIndex[server]
}

func (rf* Raft) SetNextIndex(server int, index int32) {
	rf.nextIndexMu.Lock()
	defer rf.nextIndexMu.Unlock()

	if (index >= rf.GetAfterSnapshotIndex()) {
		//oriIndex := rf.nextIndex[server]
		rf.nextIndex[server] = index
		/*if index > int32(len(rf.logs)) {
			fmt.Println("in func SetNextIndex, rf:", rf.me, "term:", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
				"index:", index, "lenOfLogs:", len(rf.logs), "oriIndex:", oriIndex, "logs:", rf.logs)
		}*/
	} else {
		//rf.nextIndex[server] = rf.GetAfterSnapshotIndex()
		rf.nextIndex[server] = index
	}
}

func (rf* Raft) GetLogs(beginIndex int32) []LogEntry {
	//fmt.Println("in func GetLogs, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogs, after voteMu rlock")
	//defer fmt.Println("in func GetLogs, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogs, before voteMu runlock")

	if int(rf.afterSnapshotIndex) > len(rf.logs) {
		//fmt.Println("in func GetLogs, afterSnapshotIndex:", rf.afterSnapshotIndex, "len:", len(rf.logs), "beginIndex:", beginIndex)
		//return []LogEntry{}
	}

	return rf.logs[beginIndex - rf.afterSnapshotIndex:]
}

func (rf* Raft) GetLogsFromTo(beginIndex int32, endIndex int32) []LogEntry {
	//fmt.Println("in func GetLogsFromTo, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogsFromTo, after voteMu rlock")
	//defer fmt.Println("in func GetLogsFromTo, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogsFromTo, before voteMu runlock")

	return rf.logs[beginIndex - rf.afterSnapshotIndex: endIndex - rf.afterSnapshotIndex]
}

/*func (rf *Raft) SetLogs() []LogEntry {

}*/

func (rf* Raft) GetLogsBehindNextIndex(server int) []LogEntry {
	return rf.GetLogs(rf.GetNextIndex(server))
}

func (rf *Raft) GetCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

func (rf *Raft) GetLogsBeforeCommitIndex() []LogEntry {
	//fmt.Println("in func GetLogsBeforeCommitIndex, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogsBeforeCommitIndex, after voteMu rlock")
	//defer fmt.Println("in func GetLogsBeforeCommitIndex, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogsBeforeCommitIndex, before voteMu runlock")

	commitIndex := rf.GetCommitIndex()
	return rf.logs[0: commitIndex + 1 - rf.afterSnapshotIndex]
}

func (rf *Raft) GetLogsAfterCommitIndex(server int) []LogEntry {
	//fmt.Println("in func GetLogsAfterCommitIndex, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogsAfterCommitIndex, after voteMu rlock")
	//defer fmt.Println("in func GetLogsAfterCommitIndex, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogsAfterCommitIndex, before voteMu runlock")

	commitIndex := rf.GetCommitIndex()
	return rf.logs[commitIndex + 1 - rf.afterSnapshotIndex:]
}



func (rf *Raft) GetLogsBehindAfterSnapshotIndex(server int) []LogEntry {
	//fmt.Println("in func GetLogsBehindAfterSnapshotIndex, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogsBehindAfterSnapshotIndex, after voteMu rlock")
	//defer fmt.Println("in func GetLogsBehindAfterSnapshotIndex, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogsBehindAfterSnapshotIndex, before voteMu runlock")

	return rf.logs[0:]
}

func (rf* Raft) AppendLogs(index int32, entries []LogEntry) {
	//fmt.Println("in func AppendLogs, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func AppendLogs, after voteMu lock")
	//defer fmt.Println("in func AppendLogs, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func AppendLogs, before voteMu unlock")

	//fmt.Println("in func AppendLogs: rf:", rf.me, "rf.Term", rf.GetCurrentTerm(), "entries:", entries)
	if index - rf.afterSnapshotIndex < 0 {
		fmt.Println("in func AppendLogs, me:", rf.me, "index:", index, "afterSnapshotIndex:", rf.afterSnapshotIndex)
	}
	len1 := len(rf.logs)
	rf.logs = append(rf.logs[:index - rf.afterSnapshotIndex], entries...)
	len2 := len(rf.logs)
	/*if len1 > len2 && rf.GetCertainState() == LEADER {
		fmt.Println("in func AppendLogs: rf:", rf.me, "rf.Term", rf.GetCurrentTerm(), "state:", rf.GetCertainState(),
			"index:", index, "logs:", rf.logs, "entries:", entries)
	}*/
	if !(int(index - rf.afterSnapshotIndex) >= len(rf.logs) && len(entries) == 0) {
		//fmt.Println("in func AppendLogs, persist")
		rf.persist()
	}
	//fmt.Println("in func AppendLogs, me:", rf.me, "len1:", len1, "len2:", len2)
	if (len1 > len2) {
		//fmt.Println("in func AppendLogs, me:", rf.me, "len1 > len2, len1:", len1, "len2:", len2)
	}
	//fmt.Println("in func AppendLogs")
	//rf.persist()

}

func (rf* Raft) PushBackLogs(entries []LogEntry) int32 {
	//fmt.Println("in func PushBackLogs, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func PushBackLogs, after voteMu lock")
	//defer fmt.Println("in func PushBackLogs, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func PushBackLogs, before voteMu unlock")

	len1 := len(rf.logs)
	if len(entries) > 0 {
		rf.logs = append(rf.logs, entries...)
		//fmt.Println("in func PushBackLogs, persist")
		rf.persist()
	}
	len2 := len(rf.logs)
	//fmt.Println("in func PushBackLogs, me:", rf.me, "len1:", len1, "len2:", len2)
	if (len1 > len2) {
		//fmt.Println("in func PushBackLogs, me:", rf.me, "len1 > len2, len1:", len1, "len2:", len2)
	}
	//rf.logs = append(rf.logs, entries...)
	//fmt.Println("in func PushBackLogs")
	//rf.persist()

	return int32(len(rf.logs))
}

func (rf* Raft) GetSingleEntry (index int32) LogEntry{
	//fmt.Println("in func GetSingleEntry, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetSingleEntry, after voteMu rlock")
	//defer fmt.Println("in func GetSingleEntry, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetSingleEntry, before voteMu runlock")

	if index >= rf.afterSnapshotIndex && int(index - rf.afterSnapshotIndex) < len(rf.logs) {
		return rf.logs[index - rf.afterSnapshotIndex]
	}
	return LogEntry{ //fake entry
		Term:    -2,
		Command: nil,
	}
}

func (rf* Raft) SetAfterSnapshotIndex(index int32) {
	//fmt.Println("in func SetAfterSnapshotIndex, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func SetAfterSnapshotIndex, after voteMu lock")
	//defer fmt.Println("in func SetAfterSnapshotIndex, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func SetAfterSnapshotIndex, before voteMu unlock")

	if rf.afterSnapshotIndex != index {
		rf.afterSnapshotIndex = index
		//fmt.Println("in func SetAfterSnapshot, afterSnapShotIndex:", index)
		if index > rf.lastApplied {
			fmt.Println("in func SetAfterSnapshotIndex, afterSnapshotIndex > lastApplied, afterSnapshotIndex:", index, "lastApplied:", rf.lastApplied)
		}
		//fmt.Println("in func SetAfterSnapshot, persist")
		rf.persist()
	}
}

func (rf* Raft) GetAfterSnapshotIndex() int32 {
	//fmt.Println("in func GetAfterSnapshotIndex, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetAfterSnapshotIndex, after voteMu rlock")
	//defer fmt.Println("in func GetAfterSnapshotIndex, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetAfterSnapshotIndex, before voteMu runlock")

	return rf.afterSnapshotIndex
}

func (rf *Raft) GetLogsBetweenAfterSnapshotAndLastApplied() []LogEntry {
	//fmt.Println("in func GetLogsBetweenAfterSnapshotAndLastApplied, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLogsBetweenAfterSnapshotAndLastApplied, after voteMu rlock")
	//defer fmt.Println("in func GetLogsBetweenAfterSnapshotAndLastApplied, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func GetLogsBetweenAfterSnapshotAndLastApplied, before voteMu runlock")

	if rf.lastApplied + 1 - rf.afterSnapshotIndex >= int32(len(rf.logs)) {
		fmt.Println("in func GetLogsBetweenAfterSnapshotAndLastApplied, me:", rf.me, "rf.lastApplied:", rf.lastApplied, "rf.afterSnapshotIndex:", rf.afterSnapshotIndex, "len:", len(rf.logs))
	}
	return rf.logs[0: rf.lastApplied + 1 - rf.afterSnapshotIndex]
}

/*func (rf* Raft) GetSingleEntryTerm (index int32) LogEntry{
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	return rf.logs[index]
}*/

func (rf* Raft) RaftStateToSnapshot(afterSnapShotIndex int32) []byte {
	/*w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)

	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()

	//logs := rf.logs[rf.afterSnapshotIndex: rf.lastApplied + 1] // kv.rf.lastAppliedIndex
	logs := rf.logs[0: rf.lastApplied - rf.afterSnapshotIndex + 1]
	rf.logs = rf.logs[rf.lastApplied - rf.afterSnapshotIndex + 1:]

	//rf.lastApplied -= rf.afterSnapshotIndex
	//rf.lastApplied = 0
	rf.afterSnapshotIndex = rf.lastApplied

	e2.Encode(rf.currentTerm)
	e2.Encode(rf.votedFor)
	e2.Encode(logs)
	e2.Encode(rf.lastApplied)
	e2.Encode(rf.afterSnapshotIndex)

	rfdata := w2.Bytes()
	return rfdata*/

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)

	//fmt.Println("in func RaftStateToSnapshot, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func RaftStateToSnapshot, after voteMu lock")
	//defer fmt.Println("in func RaftStateToSnapshot, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func RaftStateToSnapshot, before voteMu unlock")

	oriAfterSnapshotIndex := rf.afterSnapshotIndex
	rf.afterSnapshotIndex = afterSnapShotIndex
	if afterSnapShotIndex > rf.lastApplied {
		//fmt.Println("in func RaftStateToSnapshot, afterSnapshotIndex > lastApplied, afterSnapshotIndex:", afterSnapShotIndex, "lastApplied:", rf.lastApplied)
	}
	if afterSnapShotIndex - oriAfterSnapshotIndex < int32(len(rf.logs)) {
		rf.lastIncludeTerm = int32(rf.logs[afterSnapShotIndex - oriAfterSnapshotIndex].Term)
		rf.logs = rf.logs[afterSnapShotIndex - oriAfterSnapshotIndex :]
	} else {
		rf.lastIncludeTerm = -2
		rf.logs = make([]LogEntry, 0)
	}

	e2.Encode(rf.currentTerm)
	e2.Encode(rf.votedFor)
	e2.Encode(rf.logs)
	e2.Encode(rf.lastApplied)
	e2.Encode(rf.afterSnapshotIndex)
	e2.Encode(rf.lastIncludeTerm)
	rfdata := w2.Bytes()
	//fmt.Println("in func snapshot, me:", rf.me, "size of rfdata:", len(rfdata), "size of log:", len(rf.logs))

	return rfdata
}

func (rf* Raft) SetLastIncludeTerm(term int32) {
	//fmt.Println("in func SetLastIncludeTerm, before voteMu lock")
	rf.voteMu.Lock()
	//fmt.Println("in func SetLastIncludeTerm, after voteMu lock")
	//defer fmt.Println("in func SetLastIncludeTerm, after voteMu unlock")
	defer rf.voteMu.Unlock()
	//defer fmt.Println("in func SetLastIncludeTerm, before voteMu unlock")

	rf.lastIncludeTerm = term
}

func (rf* Raft) GetLastIncludeTerm() int32{
	//fmt.Println("in func GetLastIncludeTerm, before voteMu rlock")
	rf.voteMu.RLock()
	//fmt.Println("in func GetLastIncludeTerm, after voteMu rlock")
	//defer fmt.Println("in func SetLastIncludeTerm, after voteMu runlock")
	defer rf.voteMu.RUnlock()
	//defer fmt.Println("in func SetLastIncludeTerm, before voteMu runlock")

	return rf.lastIncludeTerm
}

func (rf* Raft) DoSomething(logsAfterLastApplied []LogEntry) []byte {
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()

	if rf.lastApplied < rf.afterSnapshotIndex {
		fmt.Println("in func DoSomething, lastApplied:", rf.lastApplied, "afterSnapshotIndex:", rf.afterSnapshotIndex)
	}
	logsAfterLastApplied = rf.logs[rf.lastApplied + 1 - rf.afterSnapshotIndex:]

	//fmt.Println("in func StartKVServer, SetAfterSnapshotIndex, lastApplied:", lastApplied)
	//rfdata := rf.RaftStateToSnapshot(afterSnapShotIndex)

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)

	oriAfterSnapshotIndex := rf.afterSnapshotIndex
	rf.afterSnapshotIndex = rf.lastApplied + 1

	if rf.afterSnapshotIndex - oriAfterSnapshotIndex - 1 >= 0 {
		rf.lastIncludeTerm = int32(rf.logs[rf.afterSnapshotIndex - oriAfterSnapshotIndex - 1].Term)
	} else {
		rf.lastIncludeTerm = -2
	}

	rf.logs = rf.logs[rf.afterSnapshotIndex - oriAfterSnapshotIndex :]

	e2.Encode(rf.currentTerm)
	e2.Encode(rf.votedFor)
	e2.Encode(rf.logs)
	e2.Encode(rf.lastApplied)
	e2.Encode(rf.afterSnapshotIndex)
	e2.Encode(rf.lastIncludeTerm)
	rfdata := w2.Bytes()
	//fmt.Println("in func snapshot, me:", rf.me, "size of rfdata:", len(rfdata), "size of log:", len(rf.logs))

	return rfdata

	//persister.SaveStateAndSnapshot(rfdata, dbdata)
}
