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
	"6.824_new/src/labrpc"
	"bytes"
	"encoding/gob"
	"fmt"

	//"fmt"
	"sync/atomic"

	//"fmt"
	"math/rand"
	"sync"
	"time"
)

//import "../labrpc"

// import "bytes"
// import "../labgob"

var VOTE_NIL = -1
var RPC_CALL_TIMEOUT = 1 * time.Second
var HEARTBEAT_INTERVAL = 100 * time.Millisecond
var LEADER = 1
var CANDIDATE = 2
var FOLLOWER = 3

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
	//logs map[int][]string
	logs []LogEntry

	commitIndex int
	lastApplied int

	nextIndex[] int
	matchIndex[] int

	leader int
	//LastLogIndex int

	state string

	appendEntriesCh chan bool
	voteCh chan bool
	leaderCh chan bool

	applyCh chan ApplyMsg
	
	heartbeatInterval time.Duration
	electionInterval time.Duration

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
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Candidate int
	LastLogIndex int
	LastLogTerm int
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
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int

}

type AppendEntriesReply struct {
	Term int
	Succ bool
	CommitIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {    //raft节点rf接收到投票请求后，如何去处理这个请求，并且把内容写到RequestVorereply
	// Your code here (2A, 2B).

	fmt.Println("in func RequestVote: begin")

	voteGranted := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println("in func RequestVote: voter:", rf.me, "rf.term:", rf.currentTerm, "rf.votedFor", rf.votedFor, "candidate:", args.Candidate, "candidate.Term:", args.Term)
	rf.voteCh <- true    //有人向rf发送投票请求（这个应该放在锁里面还是锁外面？放在锁里面会不会有问题？）还有，这里是可能被阻塞的
	if rf.currentTerm > args.Term { //选举人任期更早，淘汰
		//fmt.Println("in func RequestVote: rf.Term > args.Term")
		reply.Term = rf.currentTerm
		reply.VoteGranted = voteGranted
		return
	}

	if rf.currentTerm < args.Term {
		fmt.Println("in func RequestVote: rf.Term < args.Term")
		voteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.Candidate
		rf.state = "follower"

		reply.Term = rf.currentTerm
		reply.VoteGranted = voteGranted
		return
	}

	if rf.state == "follower" && (rf.votedFor == -1 || rf.votedFor == args.Candidate) && rf.lastApplied <= args.LastLogIndex {  //这里一定要rf.state == "follower"
		fmt.Println("in func RequestVote: rf.Term == args.Term and rf has not vote")
		voteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.Candidate
		rf.state = "follower"
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Println("in func AppendEntries, begin")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println("in func AppendEntries: rf:", rf.me, "rf.term:", rf.currentTerm, "leader:", args.LeaderId, "leader.Term", args.Term)
	rf.appendEntriesCh <- true  //向appendntires写入true，表示该结点收到了leader的heartbeat

	if (rf.currentTerm > args.Term) {  //该结点收到leader的heartbeat，但是自己的任期更大，拒绝
		fmt.Println("in func AppendEntries: rf.Term > args.Term")
		reply.Term = rf.currentTerm
		reply.Succ = false
	} else {  //节点接受leader的appendEntries请求
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1

		reply.Term = args.Term

		if args.PrevLogIndex >= len(rf.logs) || //leader.logs[args.PrevLogIndex]已经超出rf.logs范围
			(args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term) { //leader.logs[args.PrevLogIndex]和rf.logs同样索引的任期不同
			reply.Succ = false //可能还有欠缺，返回给leader处理？
			reply.CommitIndex = rf.commitIndex
		} else if args.Entries != nil {
			//fmt.Println("in func AppendEntries, rf:", rf.me, "args.Entries not empty")
			rf.logs = append(rf.logs[:args.PrevLogIndex + 1], args.Entries...)
			if len(rf.logs) - 1 < args.LeaderCommit {
				rf.commitIndex = len(rf.logs) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			reply.Succ = true
			reply.CommitIndex = rf.commitIndex
			go rf.commitLogs()

		} else { //heartbeat
			reply.Succ = true
			reply.CommitIndex = rf.commitIndex
			go rf.commitLogs()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)    //远程调用，假设这个方法不在这台主机上
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args* AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("in func sendAppendEntries: sender:", rf.me, "server:", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return index, term, isLeader
	}

	//cmd, _ := Encode(command)
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	//fmt.Println("term:", rf.currentTerm, "cmd:", cmd)

	rf.logs = append(rf.logs, logEntry)

	index = len(rf.logs) - 1
	term = rf.currentTerm
	isLeader = true

	return index, term, isLeader
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
	rf.mu.Lock()  //保护该结点
	defer rf.mu.Unlock()

	fmt.Println("in func LeaderElection: rf:", rf.me, "term:", rf.currentTerm)

	if rf.state != "candidate" {  //不是选举人，放弃选举
		return
	}

	rf.currentTerm++  //自增自己的任期号
	args := RequestVoteArgs{  //向别的节点发送拉票请求的req
		Term:      rf.currentTerm,
		Candidate: rf.me,
	}

	majority := int64(len(rf.peers) / 2 + 1)  //设置多数值
	voteCnt := int64(1)  //获取票数，初始化为1（自己会给自己投票）
	rf.votedFor = rf.me

	for i := 0; i < len(rf.peers); i++ {  //遍历所有节点
		if i == rf.me {
			continue
		}

		go func(voter int, args RequestVoteArgs) {  //拉票请求，向voter节点发请求
			//rf.mu.Lock()
			//defer rf.mu.Lock()
			//不能在这里，为什么？

			var reply RequestVoteReply
			ok := rf.sendRequestVote(voter, &args, &reply)  //向voter节点发送请求
			/*if !ok  {
				return
			}*/
			for !ok {
				time.Sleep(10 * time.Millisecond)
				ok = rf.sendRequestVote(voter, &args, &reply)
			}


			rf.mu.Lock()
			defer rf.mu.Unlock()
			if args.Term != rf.currentTerm {    //在期间任期发生变化，本次选举失效
				//fmt.Println("in func LeaderElection's goroutine: rf is", rf.me, "term is", reply.Term, "term change")
				return
			}

			fmt.Println("in func LeaderElection's goroutine: rf:", rf.me, "voter:", voter, "term:", reply.Term, "vote or not:", reply.VoteGranted)
			if reply.VoteGranted == false { //没有给它投票

				if reply.Term >= rf.currentTerm {  //有节点的任期更大，该结点转为follower（这里好像要等于）
					rf.currentTerm = reply.Term  //更新任期
					rf.state = "follower"  //更新到follower
					rf.votedFor = -1  //不给谁投票

					//return    //? defer
				}
			} else {

				atomic.AddInt64(&voteCnt, 1)
				if voteCnt >= majority /*&& voteCnt > 1*/{  //超过半数给该结点投票，成为leader
					fmt.Println("in func LeaderElection's goroutine: rf:", rf.me, "voter:", voter, "term:", reply.Term, "vote or not:", reply.VoteGranted, "become leader")
					rf.state = "leader"
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))

					for s := 0; s < len(rf.peers); s++ {
						rf.nextIndex[s] = len(rf.logs)
						rf.matchIndex[s] = -1
					}

					rf.leaderCh <- true  //向该结点的leaderCh通道写入，表示该节点已经是leader
				}
			}
		}(i, args)
	}

}


func (rf *Raft) heartbeat2() {
	if rf.state != "leader" {
		return
	}
	fmt.Println("in func heartbeat2, begin")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println("in func heartbeat2: rf", rf.me, "send heartbeat in term", rf.currentTerm)

	for i := range(rf.peers) {
		if (i == rf.me) {
			continue
		}
		//fmt.Println("in func heartbeat2, leader term is:", rf.currentTerm, "heartbeat sender is:", rf.me, "heartbeat receiver is:", i)
		//fmt.Println("in func heartbeat2, len(rf.nextIndex):", len(rf.nextIndex), "len(rf.matchIndex):", len(rf.matchIndex), "len(rf.logs):", len(rf.logs))
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1 //上一条日志的索引
		//if (len(args.Entries) > 0) {
			args.Entries = rf.logs[rf.nextIndex[i]:]  //需要提交的日志
		//}
		//fmt.Println("in func heartbeat2, i:", i, "len(rf.logs):", len(rf.logs), "rf.nextIndex[i]:", rf.nextIndex[i], "args.Entries:", args.Entries)

		if args.PrevLogIndex >= 0 { //存在上一条日志
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term //上一条日志的任期
		}

		args.LeaderCommit = rf.commitIndex //leader当前提交的最后一条日志的索引（也就是说，可能存在leader的有些日志还没有提交）

		go func(server int, args AppendEntriesArgs) {

			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(server, &args, &reply)

			if !ok {
				time.Sleep(10 * time.Millisecond)
				ok = rf.sendAppendEntries(server, &args, &reply)
			}

			//fmt.Println("in func heartbeat2's goroutine, ok:", ok)
			rf.handleAppendEntriesReply(server, &reply) //这里可能处理很久

		}(i, args)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		//fmt.Println("in func handle, not leader")
		return
	}

	//fmt.Println("in func handle, rf:", rf.me, "reply.Succ:", reply.Succ)

	if !reply.Succ {
		if reply.Term > rf.currentTerm {
			//fmt.Println("in func handle, rf:", rf.me, "rf.currentTerm", rf.currentTerm, "reply.Term:", reply.Term)
			rf.currentTerm = reply.Term
			rf.state = "follower"
			rf.votedFor = -1
			//rf.resetTimer()
		} else {
			rf.nextIndex[server] = reply.CommitIndex + 1 //是否需要根据reply.commitIndex处理？
			rf.heartbeat2() //这里的意思是，只要有一个节点没有正确添加日志，就向所有节点🔛再发送一次
		}
	} else {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex
		replyCnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if rf.matchIndex[i] >= rf.matchIndex[server] {
				replyCnt++
			}
		}

		//fmt.Println("in func handle, rf:", rf.me, "replyCnt:", replyCnt, "rf.commitIndex:", rf.commitIndex, "server:", server, "rf.matchIndex[server]", rf.matchIndex[server],
			//"rf.currentTerm:", rf.currentTerm/*, "rf.logs[rf.matchIndex[server]].Term:", rf.logs[rf.matchIndex[server]].Term*/)
		if replyCnt >= len(rf.peers) / 2 + 1 &&
			rf.commitIndex < rf.matchIndex[server] &&
			rf.logs[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs()
		}
	}
}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("in func commitLogs, rf:", rf.me, "rf.commitIndex:", rf.commitIndex, "len(rf.logs):", len(rf.logs), "rf.lastApplied:", rf.lastApplied)
	if (rf.commitIndex > len(rf.logs) - 1) {
		rf.commitIndex = len(rf.logs) - 1
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		//fmt.Println("in func commitLogs, rf:", rf.me, "command", rf.logs[i].Command, "commandIndex:", i)
		rf.applyCh <- ApplyMsg{
			CommandIndex: i,
			Command: rf.logs[i].Command,
			CommandValid: true,
		}
	}

	rf.lastApplied = rf.commitIndex
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
	rf.currentTerm = 0
	rf.votedFor = -1
	//rf.logs = make(map[int][]string, 0)
	rf.leader = -1
	//rf.LastLogIndex = -1
	rf.commitIndex = -1
	rf.state = "follower"

	rf.lastApplied = -1

	rf.appendEntriesCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.applyCh = applyCh

	rf.heartbeatInterval = HEARTBEAT_INTERVAL  //错开选举超时的时间，否则大家都会同时超时，同时发起选举，只会投自己，永远宣不出leader
	  	//不能太短
		//也不能设太长的超时时间，因为测试的时长只有5s，太长的超时时间，就无法测试选举功能是否正常


	go func() {
		for {
			if (rf.killed()) {
				break
			}
			//fmt.Println("in func Make: outside raft is:", rf.me, "term is:", rf.currentTerm, "state is:", rf.state, "logs is:", rf.logs)
			//fmt.Println("in func Make: outside state, raft is", rf.me, "term is", rf.currentTerm, ", state is", rf.state)
			rf.mu.Lock()
			//fmt.Println("in func Make: raft is:", rf.me, "term is:", rf.currentTerm, "state is:", rf.state, "logs is:", rf.logs)
			fmt.Println("in func Make: outside state, raft is", rf.me, "term is", rf.currentTerm, ", state is", rf.state)
			state := rf.state

			rf.electionInterval = time.Duration(500 + rand.Intn(200)) * time.Millisecond
			rf.mu.Unlock()


			//fmt.Println("in func Make: outside state, raft is", rf.me, "term is", rf.currentTerm, ", state is", rf.state)
			switch state {
				case "follower":  //当前状态为follower
					select {
					case <- rf.appendEntriesCh:  //收到心跳
						rf.mu.Lock()
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "follower", "case is <- appendEntriesCh")
						rf.mu.Unlock()

					case <- rf.voteCh:    //不用在这里写投票的操作，因为会在后面执行
						rf.mu.Lock()
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "follower", "case is <- voteCh")
						rf.mu.Unlock()

					case <-time.After(rf.electionInterval):
						//rf.currentTerm++
						rf.mu.Lock()
						rf.state = "candidate"
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "follower", "case is election timeout")
						rf.mu.Unlock()

					}
				case "candidate":  //当前该节点是选举人状态
					go rf.LeaderElection()  //执行选举goroutine
					select {
					case <-rf.appendEntriesCh:  //收到了添加日至
						rf.mu.Lock()
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "candidate", "case is <- appendEntriesCh")
						rf.mu.Unlock()

					case <-rf.voteCh:
						rf.mu.Lock()
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "candidate", "case is <- voteCh")
						rf.mu.Unlock()

					case <-rf.leaderCh:  //收到了该结点已经成为leader的信号
						rf.mu.Lock()
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "candidate", "case is <- leaderCh")
						rf.mu.Unlock()

					case <-time.After(rf.electionInterval):
						//rf.currentTerm++
						rf.mu.Lock()
						rf.state = "candidate"
						fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "candidate", "case is election timeout")
						rf.mu.Unlock()
					}
				case "leader":
					rf.mu.Lock()
					fmt.Println("in func Make: raft is", rf.me, "term is", rf.currentTerm, ", state =", "leader")
					rf.mu.Unlock()
					go rf.heartbeat2()  //这个要尽快发送，否则别的选举人还意识不到有leader，就会继续选举（问题: 是否做到了尽快？）
					time.Sleep(rf.heartbeatInterval)
			}

		}
	}()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())    //未完成

	return rf
}
