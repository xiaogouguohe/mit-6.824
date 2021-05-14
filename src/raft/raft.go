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

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) PersistTermAndVotedFor() {
	//fmt.Println("in func PersistTerm, rf:", rf.me, "term:", rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	logs := rf.GetLogs(0)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) PersistLogs() {
	//fmt.Println("in func PersistLogs, rf:", rf.me, "logs:", rf.logs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	term, votedFor := rf.GetVoteState()

	e.Encode(term)
	e.Encode(votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		//fmt.Println("in func readPersist, decode error")
		return
	} else {
		rf.SetVoteState(term, votedFor)
		rf.AppendLogs(0, logs)
		//fmt.Println("in func readPersist, after decode, term:", rf.GetCurrentTerm(), "votedFor:", rf.GetVotedFor(), "logs:", rf.GetLogs(0))
	}

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

	rf.UpdateTerm(args.Term)

	currentTerm := rf.GetCurrentTerm()
	reply.Term = currentTerm

	if currentTerm > args.Term { //选举人任期更早，淘汰
		reply.VoteGranted = false
		return
	}

	// rf.UpdateLastRecvTime()

	if rf.isLogNew(args) {
		reply.VoteGranted = false
		return
	}

	votedFor := rf.GetVotedFor()
	if votedFor == -1 || votedFor == args.Candidate {
		rf.SetVotedFor(args.Candidate)
		reply.VoteGranted = true
		rf.UpdateLastRecvTime()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.UpdateTerm(args.Term)
	currentTerm, _ := rf.GetVoteState()
	reply.Term = currentTerm

	if (currentTerm > args.Term) {  //该结点收到leader的heartbeat，但是自己的任期更大，拒绝
		reply.Succ = false
		return
	}
	rf.UpdateLastRecvTime()
	atomic.StoreUint32(&rf.state, FOLLOWER)
	reply.Succ = true

	logLen := int32(rf.GetLogsLength())
	//lastLogTerm := rf.GetLastLogTerm()
	commitIndex := rf.GetCommitIndex()

	/* rf 的上一条日志的任期 */
	termOfPrevLogIndexInRf := rf.GetSingleEntry(args.PrevLogIndex).Term

	/* args 的上一条日志索引大于 rf 的日志长度 */
	if args.PrevLogIndex > logLen /*|| args.LeaderCommit > logLen - 1*/ {
		reply.Succ = false
		reply.CommitIndex = commitIndex
		reply.Term = args.Term

	/* args 的上一条日志索引大于等于 0，且 args 的上一条日志任期不等于 rf 的上一条日志任期 */
	} else if args.PrevLogIndex >= 0 && args.PrevLogTerm != int32(termOfPrevLogIndexInRf)/*lastLogTerm*/ {
		reply.Succ = false
		reply.CommitIndex = commitIndex
		reply.Term = args.Term
		reply.ConflictTerm = termOfPrevLogIndexInRf
		i := args.PrevLogIndex
		for ; i > 0 && rf.GetSingleEntry(i).Term == termOfPrevLogIndexInRf; i-- {

		}
		reply.ConflictIndex = i
	} else {
		//if (args.PrevLogTerm == int32(args.Term)) {
		//	reply.Succ = true
		//}

		if (len(args.Entries) == 0 || args.Entries[len(args.Entries) - 1].Term == args.Term) {
			reply.Succ = true
		}

		reply.CommitIndex = args.LeaderCommit
		reply.Term = args.Term

		rf.AppendLogs(args.PrevLogIndex + 1, args.Entries)
		rf.SetCommitIndex(args.LeaderCommit)
		if rf.GetLastApplied() < rf.GetCommitIndex() {
			go rf.commitLogs()  //会不会提交到一半被覆盖？
		}
	}
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

	// Your code here (2B).
	term := rf.GetCurrentTerm()
	state := rf.GetCertainState()
	if state != LEADER {
		return -1, term, false
	}

	rf.voteMu.RLock()
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.voteMu.RUnlock()
	logEntries := []LogEntry{logEntry}

	index := rf.PushBackLogs(logEntries) - 1
	rf.cmdCh <- index

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
	atomic.StoreUint32(&rf.state, CANDIDATE)
	var currentTerm int
	for {
		state := rf.GetCertainState()
		if (rf.killed() || state != CANDIDATE) {
			return
		}

		currentTerm, _ = rf.TurnToCandidate()
		itself := rf.GetItself()

		replyCount := make(chan int, len(rf.peers))
		rand.Seed(time.Now().UnixNano())
		timeout := time.Duration(time.Duration(
			rand.Intn(500)+500) * time.Millisecond)
		go func() {
			<-time.After(timeout)
			replyCount <- -1
		}()

		rf.UpdateLastRecvTime()
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

				rf.UpdateTerm(reply.Term)

				if reply.VoteGranted {
					replyCount <- voter
				}

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
		rand.Seed(time.Now().UnixNano())
		timeout = time.Duration(time.Duration(
			rand.Intn(500)+500) * time.Millisecond)
		time.Sleep(timeout)
	}
	//
	if currentTerm == rf.GetCurrentTerm() && rf.GetCertainState() == CANDIDATE {
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
			term := rf.GetCurrentTerm()
			for {
				if rf.killed() || rf.GetCertainState() != LEADER {
					break
				}
				select {
				case <- time.After(HEARTBEAT_INTERVAL / 2):

					ok := false
					okCnt := 0
					for !ok  && rf.GetCertainState() == LEADER {
						args := AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.GetItself(),
							PrevLogIndex: rf.GetPrevLogIndex(server),
							PrevLogTerm:  rf.GetPrevLogTerm(server),
							Entries:      rf.GetLogsBehindNextIndex(server),
							LeaderCommit: rf.GetCommitIndex(),
						}

						reply := AppendEntriesReply{
							Term:          0,
							Succ:          false,
							CommitIndex:   0,
							ConflictIndex: 0,
							ConflictTerm:  0,
						}
						ok = rf.sendAppendEntries(server, &args, &reply)
						okCnt++
						if !(!ok && rf.GetCertainState() == LEADER) {
							rf.UpdateTerm(reply.Term)
							if rf.killed() || rf.GetCertainState() != LEADER || rf.GetCurrentTerm() != term {
								break
							}
							if ok && !reply.Succ {
								rf.SetNextIndex(server, reply.ConflictIndex)
							}
							break
						}
						time.Sleep(10 * time.Millisecond)
					}

				case index := <-peersChs[server]:
					for {
						args := AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.GetItself(),
							PrevLogIndex: rf.GetPrevLogIndex(server),
							PrevLogTerm:  rf.GetPrevLogTerm(server),
							Entries:      rf.GetLogsBehindNextIndex(server),
							LeaderCommit: rf.GetCommitIndex(),
						}

						reply := AppendEntriesReply{}

						ok := false
						okCnt := 0
						for !ok && rf.GetCertainState() == LEADER && rf.GetCurrentTerm() == term {
							ok = rf.sendAppendEntries(server, &args, &reply)
							okCnt++
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
						} else if ok && !reply.Succ {
							rf.SetNextIndex(server, reply.ConflictIndex)
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
			/* 通过Start，索引为index的日志条目被添加到这个领导人的日志里了 */
			index := <-rf.cmdCh
			/* 日志复制，把需要复制的日志条目的索引传递给所有节点 */
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

	if (commitIndex > lastLogIndex) {
		rf.SetCommitIndex(lastLogIndex)
		commitIndex = lastLogIndex
	}

	for i := lastApplied + 1; i <= commitIndex; i++ {
		rf.voteMu.RLock()
		rf.applyCh <- ApplyMsg{
			CommandIndex: int(i + 1),
			Command: rf.logs[i].Command,
			CommandValid: true,
		}
		rf.voteMu.RUnlock()
	}

	rf.SetLastApplied(commitIndex)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	/* 读取持久化的内容 */
	rf.readPersist(persister.ReadRaftState())
	/* 节点初始状态设为FOLLOWE */
	rf.SetCertainState(FOLLOWER)

	rf.SetCommitIndex(-1)
	rf.SetLastApplied(-1)

	/* 每次都需要重新申请一个nextIndex，否则会被该节点上次的nextIndex干扰 */
	rf.nextIndexMu.Lock()
	rf.nextIndex = make([]int32, len(rf.peers))
	rf.nextIndexMu.Unlock()

	rf.matchIndexMu.Lock()
	rf.matchIndex = make([]int32, len(rf.peers))
	rf.matchIndexMu.Unlock()

	/* 提交的时候通过这个提交 */
	rf.applyCh = applyCh

	/* 错开选举超时的时间，否则大家都会同时超时，同时发起选举，只会投自己，永远宣不出leader */
	rf.heartbeatInterval = HEARTBEAT_INTERVAL
	rf.electionInterval = time.Duration(time.Duration(rand.Intn(500)+500) * time.Millisecond)

	/* Start成功之后，也就是领导人把日志条目添加到自己的日志之后，把日志条目放入这个通道，让给其它节点发送附加日志RPC的goroutine接收 */
	rf.cmdCh = make(chan int32, 1024)

	go func() {
		for {
			if (rf.killed()) {
				break
			}

			/* 休眠选举超时时间后，起来瞄一眼 */
			time.Sleep(rf.electionInterval)

			/* 如果当前节点的状态不是FOLLOWER，就没必要判断是否需要发起选举 */
			state := rf.GetCertainState()
			if state != FOLLOWER {
				continue
			}

			/* 如果当前时间和上一次接收到有效RPC的时间之差大于选举超时时间，就发起选举 */
			now := time.Now().UnixNano()
			prev := atomic.LoadInt64(&rf.lastRecvTime)
			if time.Duration(now - prev) * time.Nanosecond >= rf.electionInterval {
				//rf.UpdateLastRecvTime()
				rf.LeaderElection()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf* Raft) GetVoteState() (int, int) {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()
	return rf.currentTerm, rf.votedFor
}

func (rf *Raft) SetVoteState(term int, votedFor int) {
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()

	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = votedFor
	}

	rf.persist()
}

func (rf* Raft) GetCurrentTerm() int {

	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()
	return rf.currentTerm
}

func (rf* Raft) GetVotedFor() int {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()
	return rf.votedFor
}

func (rf *Raft) SetVotedFor(votedFor int) {
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()
	rf.votedFor = votedFor

	rf.persist()
}

func (rf *Raft) TurnToCandidate() (int, int) {
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()
	rf.currentTerm++;
	rf.votedFor = rf.me
	rf.persist()
	return rf.currentTerm, rf.votedFor
}

func (rf *Raft) TurnToLeader() () {
	rf.nextIndexMu.Lock()
	for server := 0; server < len(rf.peers); server++ {
		rf.nextIndex[server] = int32(rf.GetLogsLength())
	}
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
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1;
		atomic.StoreUint32(&rf.state, FOLLOWER)
		rf.persist()
	}
	//rf.persist()
}

func (rf *Raft) UpdateLastRecvTime() {
	now := time.Now().UnixNano()
	atomic.StoreInt64(&rf.lastRecvTime, now)
}

func (rf *Raft) isLogNew(args *RequestVoteArgs) bool {
	lastLogIndex := int32(rf.GetLastLogIndex())
	lastLogTerm := int32(rf.GetLastLogTerm())
	return lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex)
}

func (rf* Raft) GetLogsLength() int {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	return len(rf.logs)
}

func (rf* Raft) GetLastLogIndex() int32 {

	return int32(rf.GetLogsLength() - 1)
}

func (rf* Raft) GetPrevLogIndex(server int) int32 {
	return int32(rf.GetNextIndex(server) - 1)
}

func (rf* Raft) GetLastLogTerm() int32 {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	index := len(rf.logs) - 1
	if index < 0 {
		return -1
	}
	return int32(rf.logs[index].Term)
}

func (rf* Raft) GetPrevLogTerm(server int) int32 {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	index := rf.GetPrevLogIndex(server)
	if index < 0 || int(index) >= len(rf.logs) {
		return -1
	}
	return int32(rf.logs[index].Term)
}

func (rf* Raft) SetCommitIndex(index int32) {
	atomic.StoreInt32(&rf.commitIndex, index)
}

func (rf* Raft) GetLastApplied() int32 {
	return atomic.LoadInt32(&rf.lastApplied)
}

func (rf* Raft) SetLastApplied(lastApplied int32) {
	atomic.StoreInt32(&rf.lastApplied, lastApplied)
}

func (rf *Raft) GetNextIndex(server int) int32 {
	rf.nextIndexMu.RLock()
	defer rf.nextIndexMu.RUnlock()

	
	return rf.nextIndex[server]
}

func (rf* Raft) SetNextIndex(server int, index int32) {
	rf.nextIndexMu.Lock()
	defer rf.nextIndexMu.Unlock()

	if (index >= 0) {
		rf.nextIndex[server] = index
	} else {
		rf.nextIndex[server] = 0
	}
}

func (rf* Raft) GetLogs(beginIndex int32) []LogEntry {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	if int(beginIndex) > len(rf.logs) {
		return []LogEntry{}
	}

	return rf.logs[beginIndex:]
}

func (rf* Raft) GetLogsBehindNextIndex(server int) []LogEntry {
	return rf.GetLogs(rf.GetNextIndex(server))
}

func (rf *Raft) GetCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

func (rf *Raft) GetLogsBeforeCommitIndex(server int) []LogEntry {
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	commitIndex := rf.GetCommitIndex()
	return rf.logs[0: commitIndex + 1]
}

func (rf* Raft) AppendLogs(index int32, entries []LogEntry) {
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()

	rf.logs = append(rf.logs[:index], entries...)
	rf.persist()

}

func (rf* Raft) PushBackLogs(entries []LogEntry) int32 {
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()

	rf.logs = append(rf.logs, entries...)
	rf.persist()
	return int32(len(rf.logs))
}

func (rf* Raft) GetSingleEntry (index int32) LogEntry{
	rf.voteMu.RLock()
	defer rf.voteMu.RUnlock()

	if index >= 0 && int(index) < len(rf.logs) {
		return rf.logs[index]
	}
	return LogEntry{ //fake entry
		Term:    -2,
		Command: nil,
	}
}

func (rf* Raft) SetElectionInterfal() {

}
