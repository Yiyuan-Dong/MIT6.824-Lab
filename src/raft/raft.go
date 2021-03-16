package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
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
	ConstElectionTimeoutElapse = 500
	ConstLeaderIdle            = 250 * time.Millisecond
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
	Term    	 int
	Command      interface{}
	NoOpOffset   int
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
	cv              *sync.Cond
	state           int
	timer           *time.Timer
	leaderId        int
	flagTimeUp      int
	flagStateUpdate int
	flagVoteGet     int
	applyCh         chan ApplyMsg
	isSending       []bool
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
	//var Term int
	//var isleader bool
	// Your code here (2A).
	return rf.currentTerm, rf.state == ConstStateLeader
}

func (rf *Raft) GetLastIndex() int{
	return len(rf.log) - 1
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

func Min(x, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
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
	Term      int
	Success   bool
	LastIndex int
	XTerm     int
	XLen      int
}

type UpdateStateArgs struct {
	NewTerm  int
	LeaderId int
}

func (rf *Raft) TestUpToDate(lastIndex int, lastTerm int) bool {
	myLastTerm := rf.log[len(rf.log)-1].Term
	if lastTerm < myLastTerm {
		return false
	}
	if lastTerm == myLastTerm && lastIndex < len(rf.log)-1 {
		return false
	}
	return true
}

func (rf *Raft) AppendAndSetOffset(command interface{}) {
	lastIndex := len(rf.log) - 1
	offset := rf.log[lastIndex].NoOpOffset
	if lastIndex > 0 && rf.log[lastIndex].Command == nil{
		offset++
	}
	rf.log = append(rf.log, LogStruct{Command: command, Term: rf.currentTerm, NoOpOffset: offset})
}

func (rf *Raft) AppendLogs(index int, logs []LogStruct) {
	for i := 0; i < len(logs); i++ {
		if len(rf.log) < index+i {
			log.Fatalf("[%v](%v) ERROR: index too large!\n", rf.me, rf.currentTerm)
		}
		if len(rf.log) == index+i {
			rf.log = append(rf.log, logs[i])
			continue
		}
		if rf.log[index+i].Term != logs[i].Term {
			rf.log = rf.log[:index+i]
			rf.log = append(rf.log, logs[i])
		}
	}
}

func (rf *Raft) ApplyLogs() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if rf.log[i].Command == nil { continue }
			_, _ = DPrintf("[%v](%v) has applied logs at %v, tell them %v",
				rf.me, rf.currentTerm, i, i - rf.log[i].NoOpOffset)
			rf.applyCh <- ApplyMsg{Command: rf.log[i].Command,
				CommandIndex: i - rf.log[i].NoOpOffset, CommandValid: true}
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) LeaderCommitUpdate() {
	if rf.state != ConstStateLeader {
		return
	}

	number := len(rf.peers)/2 + 1
	for TestIndex := len(rf.log) - 1; TestIndex > rf.commitIndex; TestIndex-- {
		count := 1
		if rf.log[TestIndex].Term != rf.currentTerm {
			return
		}
		for i, severMatchIndex := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if severMatchIndex >= TestIndex {
				count++
			}
		}

		if count >= number {
			rf.commitIndex = TestIndex
			rf.ApplyLogs()
			return
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("[%v](%v) Get RequestVote RPC from [%v](%v)",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(args.Term, -1)
		rf.flagStateUpdate++
		rf.cv.Signal()
		reply.Term = rf.currentTerm
	}

	if rf.TestUpToDate(args.LastLogIndex, args.LastLogTerm) &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// If grant vote, reset timer
		rf.timer.Reset(GetRandomElapse())
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
func (rf *Raft) sendRequestVote(server, me, currentTerm, lastIndex, lastTerm int) {
	_, _ = DPrintf("[%v](%v) send RequestVote RPC to [%v]", me, currentTerm, server)
	args := RequestVoteArgs{Term: currentTerm, CandidateId: me, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	var reply RequestVoteReply

	ok := false
	for !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", &args, &reply)
		if !ok {
			if rf.killed() {
				return
			}
			_, _ = DPrintf("[%v](%v) FAIL to send RequestVote RPC to [%v]", me, currentTerm, server)
			time.Sleep(100 * time.Millisecond)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("[%v](%v) Get RequestVote Reply from [%v](%v), say %v",
		rf.me, rf.currentTerm, server, reply.Term, reply.VoteGranted)

	if reply.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(reply.Term, -1)
		rf.flagStateUpdate++
		rf.cv.Signal()
		return
	}

	if currentTerm == rf.currentTerm && rf.state == ConstStateCandidate && reply.VoteGranted {
		rf.flagVoteGet++
		rf.cv.Signal()
	}

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("[%v](%v) Get AppendEntries RPC from [%v](%v)",
		rf.me, rf.currentTerm, args.LeaderId, args.Term)

	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false, LastIndex: len(rf.log) - 1}

	if args.Term < rf.currentTerm {
		return
	}

	rf.timer.Reset(GetRandomElapse()) // args.Term >= rf.currentTerm, must be leader

	if args.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(args.Term, args.LeaderId)
		rf.flagStateUpdate++
		rf.cv.Signal()
		reply.Term = rf.currentTerm
	}

	if rf.state == ConstStateCandidate {
		rf.state = ConstStateFollower
		rf.leaderId = args.LeaderId
		rf.flagStateUpdate++
		rf.cv.Signal()
	}

	if len(rf.log)-1 < args.PrevLogIndex {
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		XLen := 0
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term == reply.XTerm {
				XLen++
			} else {
				break
			}
		}
		reply.XLen = XLen
		return
	}

	rf.AppendLogs(args.PrevLogIndex+1, args.Entries)
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
	}
	rf.ApplyLogs()

	return
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isSending[server] || rf.state != ConstStateLeader {
		return
	}
	rf.isSending[server] = true
	defer func() { rf.isSending[server] = false }()

	for {
		prevIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.log[prevIndex].Term

		entries := rf.log[rf.nextIndex[server]:]

		_, _ = DPrintf("[%v](%v) send AppendEntries RPC to [%v], [%v:%v]",
			rf.me, rf.currentTerm, server, rf.nextIndex[server], len(rf.log))

		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevIndex,
			PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: rf.commitIndex}
		var reply AppendEntriesReply

		rf.mu.Unlock()

		ok := false
		for !ok {
			ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				if rf.killed() {
					rf.mu.Lock()
					return
				}
				_, _ = DPrintf("[%v](%v) FAIL to send AppendEntries RPC to [%v]", rf.me, rf.currentTerm, server)
				time.Sleep(100 * time.Millisecond)
			}
		}

		rf.mu.Lock()
		_, _ = DPrintf("[%v](%v) Get AppendEntries Reply from [%v](%v), say %v",
			rf.me, rf.currentTerm, server, reply.Term, reply.Success)

		if rf.currentTerm < reply.Term {
			rf.StartNewFollowerTerm(reply.Term, -1)
			rf.flagStateUpdate++
			rf.cv.Signal()
			return
		}

		if rf.state != ConstStateLeader {
			return
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] += len(args.Entries)
			_, _ = DPrintf("[%v](%v) Server: %v, matchIndex: %v, nextIndex: %v",
				rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
		} else {
			if reply.LastIndex < args.PrevLogIndex {
				rf.nextIndex[server] = reply.LastIndex + 1
			} else {
				rf.nextIndex[server] -= reply.XLen
			}
		}

		if rf.nextIndex[server] == len(rf.log) {
			rf.LeaderCommitUpdate()
			return
		}
	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != ConstStateLeader {
		return 0, 0, false
	}

	rf.AppendAndSetOffset(command)
	retIndex := rf.GetLastIndex() - rf.log[rf.GetLastIndex()].NoOpOffset
	_, _ = DPrintf("[%v](%v) Get command %v, return %v, actually %v",
		rf.me, rf.currentTerm, command, retIndex, len(rf.log) - 1)
	rf.SendEntries()
	rf.timer.Reset(ConstLeaderIdle)

	return retIndex, rf.currentTerm, true

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

func GetRandomElapse() time.Duration {
	nsCount := ConstElectionTimeoutElapse + rand.Intn(ConstElectionTimeoutElapse)
	return time.Duration(nsCount) * time.Millisecond / time.Nanosecond
}

func (rf *Raft) StartNewFollowerTerm(newTerm int, leaderId int) {
	//DPrintf("[%v](%v) will update into Term %v\n", rf.me, rf.currentTerm, newTerm)
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.state = ConstStateFollower
	rf.leaderId = leaderId
}

func (rf *Raft) SendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.sendAppendEntries(index) //Empty entry here
	}
}

func (rf *Raft) ResetFlag() {
	rf.flagStateUpdate = 0
	rf.flagTimeUp = 0
	rf.flagVoteGet = 0
}

func (rf *Raft) ShowFlag() {
	_, _ = DPrintf("[%v](%v) is signaled, Flag: Time(%v), Vote(%v), Update(%v)",
		rf.me, rf.currentTerm, rf.flagTimeUp, rf.flagVoteGet, rf.flagStateUpdate)
}

func (rf *Raft) ProcessFollower() {
	// rf.mu.Lock() before the func
	_, _ = DPrintf("[%v](%v) now turn into follower state\n", rf.me, rf.currentTerm)
	rf.ResetFlag()
	oldTerm := rf.currentTerm

	rf.timer.Reset(GetRandomElapse())
	rf.cv.Wait()
	rf.ShowFlag()

	if oldTerm == rf.currentTerm {
		rf.state = ConstStateCandidate
	}
	return
}

func (rf *Raft) ProcessCadidate() {
	// rf.mu.Lock() before the func
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetFlag()
	rf.flagVoteGet = 1
	_, _ = DPrintf("[%v](%v) now turn into candidate state\n", rf.me, rf.currentTerm)

	numbers := len(rf.peers)/2 + 1
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.sendRequestVote(index, rf.me, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
	}

	rf.timer.Reset(GetRandomElapse())

	for {
		rf.cv.Wait()
		rf.ShowFlag()

		if rf.flagStateUpdate > 0 || rf.flagTimeUp > 0 {
			break
		}
		_, _ = DPrintf("[%v](%v) current Vote Count: %v", rf.me, rf.currentTerm, rf.flagVoteGet)
		if rf.flagVoteGet >= numbers {
			break
		}
		//log.Fatalf("ERROR: [%v](%v) no available flag but break!\n", rf.me, rf.currentTerm)
	}

	if rf.flagStateUpdate > 0 || rf.flagTimeUp > 0 { // If time up then begin a new candidate Term
		return
	}

	if rf.flagVoteGet >= numbers {
		rf.state = ConstStateLeader
		return
	}

	log.Fatalf("ERROR: [%v](%v) break wait but no available state!\n", rf.me, rf.currentTerm)
	return
}

func (rf *Raft) ProcessLeader() {
	// rf.mu.Lock() before the func
	_, _ = DPrintf("[%v](%v) now turn into leader state\n", rf.me, rf.currentTerm)
	rf.AppendAndSetOffset(nil) // no-op entry in the paper
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0            // In the paper index begin from 1, so initialized to 0
		rf.nextIndex[i] = len(rf.log)
	}

	for {
		rf.ResetFlag()

		rf.SendEntries()
		rf.timer.Reset(ConstLeaderIdle)

		rf.cv.Wait()
		rf.ShowFlag()
		if rf.flagStateUpdate > 0 {
			break
		}
		if rf.killed() {
			return
		}
	}

	return
}

func (rf *Raft) RaftMain() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			_, _ = DPrintf("[%v](%v) main goroutine gracefully exit\n", rf.me, rf.currentTerm)
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

func (rf *Raft) TimeClick() {
	for {
		<-rf.timer.C

		rf.mu.Lock()
		rf.cv.Signal()
		rf.flagTimeUp++
		if rf.killed() {
			_, _ = DPrintf("[%v](%v) time click goroutine will exit", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.isSending = make([]bool, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.me = me
	rf.mu = sync.Mutex{}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.state = ConstStateFollower
	rf.timer = time.NewTimer(3600 * time.Second)
	rf.cv = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.lastApplied = 0 // So rf.log[0] should never be applied
	rf.commitIndex = 0
	rf.log = []LogStruct{{Term: 0, Command: nil, NoOpOffset: 0}}
	rf.votedFor = -1
	go rf.TimeClick()

	go rf.RaftMain()

	return rf
}
