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
	"math/rand"
	"runtime"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	ConstStateLeader           = 0
	ConstStateFollower         = 1
	ConstStateCandidate        = 2
	ConstElectionTimeoutElapse = 600
	ConstLeaderIdle            = 300 * time.Millisecond
)

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

type LogStruct struct {
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
	// vars create by myself...
	state       int
	timer       *time.Timer
	updateState chan int
	leaderId    int
	// variables mentioned in Figure 2
	currentTerm int
	votedFor    int
	log         []LogStruct
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	return rf.currentTerm, rf.state == ConstStateLeader
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogStruct
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type UpdateStateArgs struct {
	NewTerm  int
	LeaderId int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	_, _ = DPrintf("[%v](%v) Get RequestVote RPC from [%v](%v)",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)
	updateState := false
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(args.Term, -1)
		updateState = true
		rf.votedFor = args.CandidateId
		rf.ResetTimer()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.ResetTimer()
	}

	rf.mu.Unlock()
	if updateState {
		rf.updateState <- 1
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
func (rf *Raft) sendRequestVote(server int, me int, currentTerm int, ch chan int) {
	DPrintf("[%v](%v) send RequestVote RPC to [%v]", me, currentTerm, server)
	args := RequestVoteArgs{Term: currentTerm, CandidateId: me}
	var reply RequestVoteReply

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok{
		DPrintf("[%v](%v) FAIL to send RequestVote RPC to [%v]", me, currentTerm, server)
		return
	}

	rf.mu.Lock()
	DPrintf("[%v](%v) Get RequestVote Reply from [%v](%v), he say %v",
		rf.me, rf.currentTerm, server, reply.Term, reply.VoteGranted)
	if reply.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(reply.Term, -1)
		rf.mu.Unlock()
		rf.updateState <- 1
		return
	}
	rf.mu.Unlock()

	if reply.VoteGranted == true {
		ch <- 1
	}

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	updateState := false

	rf.mu.Lock()

	DPrintf("[%v](%v) Get AppendEntries RPC from [%v], his term %v",
		rf.me, rf.currentTerm, args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(args.Term, args.LeaderId)
		updateState = true
	}

	if args.Term == rf.currentTerm {
		if rf.state == ConstStateCandidate {
			rf.state = ConstStateFollower
			rf.leaderId = args.LeaderId
			updateState = true
		}
	}

	rf.ResetTimer()
	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}

	rf.mu.Unlock()

	if updateState {
		rf.updateState <- 1
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	DPrintf("[%v](%v) send AppendEntries RPC to [%v]", rf.me, rf.currentTerm, server)
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: -1,
		PrevLogTerm: -1, Entries: nil, LeaderCommit: -1}
	var reply AppendEntriesReply
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok{
		DPrintf("[%v](%v) FAIL to send AppendEntries RPC to [%v]", rf.me, rf.currentTerm, server)
		return
	}


	rf.mu.Lock()
	DPrintf("[%v](%v) Get AppendEntries Reply from [%v](%v), he say %v",
		rf.me, rf.currentTerm, server, reply.Term, reply.Success)
	if rf.currentTerm < reply.Term {
		rf.StartNewFollowerTerm(reply.Term, -1)
		rf.mu.Unlock()
		rf.updateState <- 1
		return
	}
	rf.mu.Unlock()
	return

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

func (rf *Raft) ResetTimer() {
	nsCount := ConstElectionTimeoutElapse + rand.Intn(ConstElectionTimeoutElapse)
	rf.timer.Reset(time.Duration(nsCount) * time.Millisecond / time.Nanosecond)

FOR_TIMER:
	for {
		select {
		case <-rf.timer.C:
			// Do nothing
		default:
			break FOR_TIMER
		}
	}
}

func (rf *Raft) StartNewFollowerTerm(newTerm int, leaderId int) {
	//DPrintf("[%v](%v) will update into term %v\n", rf.me, rf.currentTerm, newTerm)
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.state = ConstStateFollower
	rf.leaderId = leaderId
}

func (rf *Raft) SendEmptyEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.sendAppendEntries(index) //Empty entry here
	}
}

func (rf *Raft) ProcessFollower() {
	// rf.mu.Lock() before the func
	_, _ = DPrintf("[%v](%v) now turn into follower state\n", rf.me, rf.currentTerm)
	rf.ResetTimer()
	oldTerm := rf.currentTerm
	rf.mu.Unlock()

	select {
	case <-rf.updateState:
		break
	case <-rf.timer.C:
		break
	}

	rf.mu.Lock()
	if oldTerm == rf.currentTerm {
		rf.state = ConstStateCandidate
	}
	return
}

func (rf *Raft) ProcessCadidate() {
	// rf.mu.Lock() before the func
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetTimer()
	_, _ = DPrintf("[%v](%v) now turn into candidate state\n", rf.me, rf.currentTerm)
	ch := make(chan int)
	replyCount := 1 // I have voted for myself
	numbers := len(rf.peers)/2 + 1
	me := rf.me

	for index := range rf.peers {
		if index == me {
			continue
		}
		go rf.sendRequestVote(index, rf.me, rf.currentTerm, ch)
	}
	rf.mu.Unlock()


FOR_CANDIDATE:
	for {
		select {
		case <-rf.updateState:
			break FOR_CANDIDATE
		case <-ch:
			replyCount++
			DPrintf("[%v] count: %v", me, replyCount)
			if replyCount == numbers {
				break FOR_CANDIDATE
			}
		case <-rf.timer.C:
			break FOR_CANDIDATE
		}
	}

	rf.mu.Lock()
	if rf.state == ConstStateFollower {
		return
	}

	if replyCount == numbers {
		rf.state = ConstStateLeader
		return
	}

	// Else begin a new candidate term
	return
}

func (rf *Raft) ProcessLeader() {
	// rf.mu.Lock() before the func
	_, _ = DPrintf("[%v](%v) now (again) turn into leader state\n", rf.me, rf.currentTerm)
	rf.SendEmptyEntries()

	rf.timer.Reset(ConstLeaderIdle)
	rf.mu.Unlock()
	defer rf.mu.Lock()

	select {
	case <-rf.updateState:
		return
	case <-rf.timer.C:
		return
	}

}

func (rf *Raft) RaftMain() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			DPrintf("[%v](%v) GG! now have %v goroutine\n", rf.me, rf.currentTerm, runtime.NumGoroutine())
			break
		}

		if rf.state == ConstStateFollower {
			rf.ProcessFollower()
			continue
		}

		if rf.state == ConstStateCandidate {
			rf.ProcessCadidate()
			continue
		}

		if rf.state == ConstStateLeader {
			rf.ProcessLeader()
			continue
		}
	}
	rf.mu.Unlock()
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.state = ConstStateFollower
	rf.timer = time.NewTimer(100 * time.Second)
	rf.updateState = make(chan int)

	go rf.RaftMain()

	return rf
}
