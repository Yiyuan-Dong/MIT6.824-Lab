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
	"../labgob"
	"../labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ConstStateLeader           = 0
	ConstStateFollower         = 1
	ConstStateCandidate        = 2
	ConstElectionTimeoutElapse = 400 // would be used as (X + rand.intn(X))
	ConstLeaderIdle            = 200 * time.Millisecond
	ConstRetryGap              = 200 * time.Millisecond
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
	IsLeader     bool
	Snapshot     []byte
}

type LogStruct struct {
	Term       int
	Command    interface{}
	NoOpOffset int
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
	appendOpId      int
	// variables mentioned in Figure 2
	currentTerm  int         //Persist
	votedFor     int         //Persist
	log          []LogStruct //Persist
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	deleteLogNum int 		 //Persist
	isApplying   bool
	needSnapshot int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var Term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == ConstStateLeader
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.leaderId
}

func (rf *Raft) GetLastIndex() int {
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.deleteLogNum) != nil {
		log.Fatal("Error while encoding")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, deleteLogNum int
	var logs []LogStruct
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&deleteLogNum) != nil {
		log.Fatal("Error while decode")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.deleteLogNum = deleteLogNum
		rf.lastApplied = rf.deleteLogNum - 1
	}
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
	Term         int
	Success      bool
	LastIndex    int
	LastApplied  int
	XTerm        int
	XLen         int
	DeleteLogNum int
}

type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int
	LastIndex    int
	DeleteLogNum int
	State        []byte
	Snapshot     []byte
}

type InstallSnapshotReply struct {
	Term    	 int
	LastApplied  int
	Success 	 bool
}

type UpdateStateArgs struct {
	NewTerm  int
	LeaderId int
}

/**
 * When someone request for a vote, test if he is update enough
 */
func (rf *Raft) TestUpToDate(lastIndex int, lastTerm int) bool {
	myLastTerm := rf.log[len(rf.log)-1].Term
	if lastTerm < myLastTerm {
		return false
	}
	if lastTerm == myLastTerm && lastIndex < len(rf.log)-1+rf.deleteLogNum {
		return false
	}
	return true
}

func (rf *Raft) AppendAndSetOffset(command interface{}) {
	lastIndex := rf.GetLastIndex()
	offset := rf.log[lastIndex].NoOpOffset
	if lastIndex+rf.deleteLogNum > 0 && rf.log[lastIndex].Command == nil { // that means: log[1].offset == 0
		offset++
	}
	rf.log = append(rf.log, LogStruct{Command: command, Term: rf.currentTerm, NoOpOffset: offset})
}

func (rf *Raft) AppendLogs(index int, logs []LogStruct) { // here index has (- rf.DeleteLogNum)
	for i := 0; i < len(logs); i++ {
		if len(rf.log) < index+i {
			log.Fatalf("[%v](%v) ERROR: index too large!\n", rf.me, rf.currentTerm)
		}
		if len(rf.log) == index+i {
			rf.log = append(rf.log, logs[i:]...)
			return
		}
		if rf.log[index+i].Term != logs[i].Term {
			rf.log = append(rf.log[:index+i], logs[i:]...)
			return
		}
	}
}

func (rf *Raft) ApplyLogs() {
	if rf.commitIndex > rf.lastApplied && !rf.isApplying {
		go func() {
			rf.mu.Lock()
			rf.isApplying = true
			defer rf.mu.Unlock()
			defer func() { rf.isApplying = false }()

			for {
				if rf.needSnapshot > 0 {
					applyMsg := ApplyMsg{Snapshot: rf.persister.ReadSnapshot()}
					rf.mu.Unlock()

					rf.applyCh <- applyMsg

					rf.mu.Lock()
					rf.needSnapshot -= 1
					continue
				}

				i := rf.lastApplied + 1 - rf.deleteLogNum
				if i > rf.commitIndex-rf.deleteLogNum {
					return
				}
				rf.lastApplied += 1
				if rf.log[i].Command == nil {
					continue
				}

				_, _ = DPrintf("[%v](%v) will apply %v at %v(+%v), tell them %v",
					rf.me, rf.currentTerm,
					rf.log[i].Command, i,
					rf.deleteLogNum, i+rf.deleteLogNum-rf.log[i].NoOpOffset)

				applyMsg := ApplyMsg{
					Command: rf.log[i].Command,
					CommandIndex: i - rf.log[i].NoOpOffset + rf.deleteLogNum,
					CommandValid: true,
					IsLeader: rf.state == ConstStateLeader,
					Snapshot: nil}

				rf.mu.Unlock()

				rf.applyCh <- applyMsg

				rf.mu.Lock()
			}
		}()

	}
}

/**
 * Whenever a follower says he record some logs, leader should
 * consider whether I can commit more log
 */
func (rf *Raft) LeaderCommitUpdate() {
	if rf.state != ConstStateLeader {
		return
	}
	number := len(rf.peers)/2 + 1
	for TestIndex := len(rf.log) - 1; TestIndex > rf.commitIndex-rf.deleteLogNum; TestIndex-- {
		count := 1 // count myself

		// Do not commit older term
		if rf.log[TestIndex].Term != rf.currentTerm {
			return
		}

		for i, severMatchIndex := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if severMatchIndex >= TestIndex+rf.deleteLogNum {
				count++
			}
		}

		if count >= number {
			rf.commitIndex = TestIndex + rf.deleteLogNum
			rf.ApplyLogs()
			// Immediately tell followers new commit. Help to pass test?
			rf.flagTimeUp++
			rf.cv.Signal()
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
		rf.me, rf.currentTerm,
		args.CandidateId, args.Term)
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(args.Term, -1)
		rf.flagStateUpdate++
		rf.cv.Signal()
		reply.Term = rf.currentTerm
		rf.persist()
	}

	if rf.TestUpToDate(args.LastLogIndex, args.LastLogTerm) &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// If grant vote, reset timer
		rf.timer.Reset(GetRandomElapse())
		rf.persist()
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
	args := RequestVoteArgs{
		Term: currentTerm,
		CandidateId: me,
		LastLogIndex: lastIndex,
		LastLogTerm: lastTerm}
	var reply RequestVoteReply

	ok := false
	for !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", &args, &reply)
		if !ok {
			if rf.killed() {
				return
			}
			_, _ = DPrintf("[%v](%v) fail to send RequestVote RPC to [%v]", me, currentTerm, server)
			time.Sleep(ConstRetryGap)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("[%v](%v) Get RequestVote Reply from [%v](%v), say %v",
		rf.me, rf.currentTerm,
		server, reply.Term,
		reply.VoteGranted)

	if reply.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(reply.Term, -1)
		rf.flagStateUpdate++
		rf.cv.Signal()
		rf.persist()
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

	_, _ = DPrintf("[%v](%v) Get AppendEntries RPC from [%v](%v). [%v:%v](-%v)",
		rf.me,
		rf.currentTerm,
		args.LeaderId, args.Term,
		args.PrevLogIndex+1,
		args.PrevLogIndex+1+len(args.Entries),
		rf.deleteLogNum)

	*reply = AppendEntriesReply{
		Term: rf.currentTerm,
		Success: false,
		LastIndex: len(rf.log) - 1 + rf.deleteLogNum,
		DeleteLogNum: rf.deleteLogNum,
		LastApplied: rf.lastApplied}

	if args.Term < rf.currentTerm {
		return
	}

	rf.timer.Reset(GetRandomElapse()) // args.Term >= rf.currentTerm, must be leader

	if args.Term > rf.currentTerm {
		rf.StartNewFollowerTerm(args.Term, args.LeaderId)
		rf.flagStateUpdate++
		rf.cv.Signal()
		reply.Term = rf.currentTerm
		rf.persist()
	}

	if rf.state == ConstStateCandidate {
		rf.state = ConstStateFollower
		rf.leaderId = args.LeaderId
		rf.flagStateUpdate++
		rf.cv.Signal()
	}

	if len(rf.log)-1+rf.deleteLogNum < args.PrevLogIndex {
		return
	}
	if rf.deleteLogNum > args.PrevLogIndex {
		return
	}

	if rf.log[args.PrevLogIndex-rf.deleteLogNum].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex-rf.deleteLogNum].Term
		XLen := 0
		for i := args.PrevLogIndex - rf.deleteLogNum; i >= 0; i-- {
			if rf.log[i].Term == reply.XTerm {
				XLen++
			} else {
				break
			}
		}
		reply.XLen = XLen
		return
	}

	rf.AppendLogs(args.PrevLogIndex+1-rf.deleteLogNum, args.Entries) // need to (- rf.DeleteLogNum)
	rf.persist()

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1+rf.deleteLogNum)
		rf.ApplyLogs()
	}

	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("[%v](%v) Get InstallSnapshot RPC from [%v](%v), lastIndex: %v, deleteNum: %v",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LastIndex, args.DeleteLogNum)
	_, _ = DPrintf("[%v](%v) lastApplied: %v, deleteLogNum: %v, log: %v",
		rf.me, rf.currentTerm, rf.lastApplied, rf.deleteLogNum, rf.log)

	*reply = InstallSnapshotReply{
		Term: rf.currentTerm,
		Success: false,
		LastApplied: rf.lastApplied}

	if args.DeleteLogNum - 1 < rf.lastApplied{
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term == rf.currentTerm &&
		args.LastIndex <= rf.deleteLogNum+len(rf.log)-1 &&
		rf.log[len(rf.log) - 1].Term == rf.currentTerm{ // This snapshot is old
		return
	}

	reply.Success = true
	_, _ = DPrintf("[%v](%v) will install snapshot", rf.me, rf.currentTerm)

	rf.persister.SaveStateAndSnapshot(args.State, args.Snapshot)
	rf.readPersist(args.State)
	rf.timer.Reset(GetRandomElapse())

	rf.commitIndex = Max(rf.commitIndex, rf.deleteLogNum-1)
	rf.needSnapshot += 1

	if rf.state != ConstStateFollower {
		rf.state = ConstStateFollower
		rf.flagStateUpdate++
	}
	rf.leaderId = args.LeaderId
	rf.cv.Signal()
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1+rf.deleteLogNum)
	}
	rf.ApplyLogs()

	return
}

func (rf *Raft) SendAppendOrSnapshot(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != ConstStateLeader {
		return
	}
	currentTerm := rf.currentTerm

	for {
		me := rf.me
		peerServer := rf.peers[server]

		if rf.nextIndex[server]-1 < rf.deleteLogNum { // should be able to read prevIndex
			// Will call InstallSnapshot RPC!
			lastIndex := rf.deleteLogNum + len(rf.log) - 1

			_, _ = DPrintf("[%v](%v) send InstallSnapshot RPC to [%v]",
				rf.me, rf.currentTerm, server)
			args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIndex: lastIndex,
				LeaderCommit: rf.commitIndex,
				DeleteLogNum: rf.deleteLogNum,
				State: rf.persister.ReadRaftState(),
				Snapshot: rf.persister.ReadSnapshot()}
			var reply InstallSnapshotReply

			rf.mu.Unlock()

			ok := peerServer.Call("Raft.InstallSnapshot", &args, &reply)

			rf.mu.Lock()

			if !ok {
				_, _ = DPrintf("[%v](%v) fail to send InstallSnapshot RPC to [%v]",
					me, currentTerm, server)
				return
			}

			_, _ = DPrintf("[%v](%v) Get InstallSnapshot Reply from [%v](%v), say %v",
				rf.me, rf.currentTerm, server, reply.Term, reply.Success)

			if rf.currentTerm < reply.Term {
				rf.StartNewFollowerTerm(reply.Term, -1)
				rf.flagStateUpdate++
				rf.cv.Signal()
				rf.persist()
				return
			}

			if currentTerm != rf.currentTerm {
				return
			}

			if reply.Success {
				rf.matchIndex[server] = Max(rf.matchIndex[server], lastIndex)
				rf.nextIndex[server] = Max(rf.matchIndex[server], lastIndex+1)
				_, _ = DPrintf("[%v](%v) Server: %v, matchIndex: %v, nextIndex: %v",
					rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
			}

			if reply.LastApplied + 1 > rf.nextIndex[server]{
				rf.nextIndex[server] = reply.LastApplied + 1
				continue
			}

			return

		} else {
			prevIndex := rf.nextIndex[server] - 1
			prevLogTerm := rf.log[prevIndex-rf.deleteLogNum].Term

			entries := make([]LogStruct, len(rf.log)+rf.deleteLogNum-rf.nextIndex[server])
			copy(entries, rf.log[rf.nextIndex[server]-rf.deleteLogNum:])

			_, _ = DPrintf("[%v](%v) send AppendEntries RPC to [%v], [%v:%v](-%v)",
				rf.me, rf.currentTerm, server, rf.nextIndex[server],
				len(rf.log)+rf.deleteLogNum, rf.deleteLogNum)

			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm: prevLogTerm,
				Entries: entries,
				LeaderCommit: rf.commitIndex}
			var reply AppendEntriesReply

			rf.mu.Unlock()

			ok := peerServer.Call("Raft.AppendEntries", &args, &reply)

			rf.mu.Lock()

			if !ok {
				_, _ = DPrintf("[%v](%v) fail to send AppendEntries RPC to [%v]", me, currentTerm, server)
				return
			}

			_, _ = DPrintf("[%v](%v) Get AppendEntries Reply from [%v](%v), say %v",
				rf.me, rf.currentTerm, server, reply.Term, reply.Success)

			if rf.currentTerm < reply.Term {
				rf.StartNewFollowerTerm(reply.Term, -1)
				rf.flagStateUpdate++
				rf.cv.Signal()
				rf.persist()
				return
			}

			if currentTerm != rf.currentTerm {
				return
			}

			if reply.Success {
				rf.matchIndex[server] = Max(rf.matchIndex[server], prevIndex+len(args.Entries))
				rf.nextIndex[server] = Max(rf.nextIndex[server], prevIndex+1+len(args.Entries))
				_, _ = DPrintf("[%v](%v) Server: %v, matchIndex: %v, nextIndex: %v",
					rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
			} else {
				if rf.matchIndex[server] >= args.PrevLogIndex {
					return
				}

				if reply.LastIndex < args.PrevLogIndex {
					rf.nextIndex[server] = Min(rf.nextIndex[server], reply.LastIndex+1)
				} else {
					newPrev := prevIndex-reply.XLen
					for {
						next := newPrev - rf.deleteLogNum + 1
						if next < 0{
							break
						}
						if rf.log[next].Term != reply.XTerm{
							break
						}
						newPrev += 1
					}
					rf.nextIndex[server] = Min(rf.nextIndex[server], newPrev+1)
				}

				if reply.LastApplied >= rf.nextIndex[server] {
					rf.nextIndex[server] = reply.LastApplied + 1
				}
			}

			if rf.matchIndex[server] == len(rf.log)+rf.deleteLogNum-1 {
				rf.LeaderCommitUpdate()
				return
			}
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
	rf.persist()

	retIndex := len(rf.log) - 1 - rf.log[len(rf.log)-1].NoOpOffset + rf.deleteLogNum
	_, _ = DPrintf("[%v](%v) Get command %v, save at %v(+%v), tell them %v",
		rf.me, rf.currentTerm, command, len(rf.log)-1, rf.deleteLogNum, retIndex)

	rf.timer.Reset(40 * time.Millisecond)

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
		go rf.SendAppendOrSnapshot(index) //Empty entry here
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

func (rf *Raft) ProcessCandidate() {
	// rf.mu.Lock() before the func
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetFlag()
	rf.flagVoteGet = 1
	rf.persist()
	_, _ = DPrintf("[%v](%v) now turn into candidate state\n", rf.me, rf.currentTerm)

	numbers := len(rf.peers)/2 + 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVote(
			peer,
			rf.me,
			rf.currentTerm,
			len(rf.log)-1+rf.deleteLogNum,
			rf.log[len(rf.log)-1].Term)
	}

	rf.timer.Reset(GetRandomElapse())

	for {
		rf.cv.Wait()
		rf.ShowFlag()

		if rf.flagStateUpdate > 0 || rf.flagTimeUp > 0 {
			break
		}
		_, _ = DPrintf("[%v](%v) current Vote Count: %v",
			rf.me, rf.currentTerm, rf.flagVoteGet)
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

	return
}

func (rf *Raft) ProcessLeader() {
	// rf.mu.Lock() before the func
	_, _ = DPrintf("[%v](%v) now turn into leader state\n", rf.me, rf.currentTerm)
	rf.AppendAndSetOffset(nil) // no-op entry in the paper
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0 // In the paper index begin from 1, so initialized to 0
		rf.nextIndex[i] = len(rf.log) + rf.deleteLogNum
	}

	for {
		rf.ResetFlag()

		rf.SendEntries()
		rf.timer.Reset(ConstLeaderIdle)

		rf.cv.Wait()
		rf.ShowFlag()
		if rf.flagStateUpdate > 0 {
			return
		}
		if rf.killed() {
			return
		}
	}
}

func (rf *Raft) RaftMain() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			_, _ = DPrintf("[%v](%v) main goroutine gracefully exit\n", rf.me, rf.currentTerm)
			break
		}

		switch rf.state {
		case ConstStateFollower:
			rf.ProcessFollower()
		case ConstStateCandidate:
			rf.ProcessCandidate()
		case ConstStateLeader:
			rf.ProcessLeader()
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
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.state = ConstStateFollower
	rf.timer = time.NewTimer(3600 * time.Second)
	rf.cv = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.lastApplied = 0 // So rf.log[0] should never be applied
	rf.commitIndex = 0
	rf.log = []LogStruct{{Term: 0, Command: nil, NoOpOffset: 0}} // real log begin from 1
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.appendOpId = 0
	rf.deleteLogNum = 0
	rf.isApplying = false
	rf.needSnapshot = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	if len(rf.log) > 1 {
		_, _ = DPrintf("[%v](%v): restart, votedFor: %v, log: %v, deleteLogNum: %v, ",
			me, rf.currentTerm, rf.votedFor, rf.log, rf.deleteLogNum)
	} else {
		_, _ = DPrintf("[%v](%v): pure start", me, rf.currentTerm)
	}

	rf.mu.Unlock()

	go rf.TimeClick()

	go rf.RaftMain()

	return rf
}

func (rf *Raft) Delete(index int) {
	realIndex := -1
	for i := 0; i < len(rf.log); i++ {
		if i+rf.deleteLogNum-rf.log[i].NoOpOffset == index &&
			rf.log[i].Command != nil {
			realIndex = i
		}
	}

	if rf.lastApplied-rf.deleteLogNum < realIndex {
		// TODO: Can I delete it?
		// 这里情况有亿点点复杂，还是用中文吧
		// 如果在我apply了一个log之后，我又InstallSnapShot，这时我的lastApplied
		// 会被重置为rf.DeleteLogNum - 1。但是KVServer可能会根据之前apply的log让我
		// 删掉lastApplied之后的log，这时不能删
		log.Fatalf("[%v] %v %v %v", rf.me, rf.lastApplied, rf.deleteLogNum, realIndex)
		return
	}

	if realIndex >= 1 {
		realIndex -= 1 // keep at least one log
		rf.deleteLogNum += 1 + realIndex
		DPrintf("[%v](%v) delete from %v(real:%v), deleteNum: %v",
			rf.me, rf.currentTerm, index, realIndex, rf.deleteLogNum)
		rf.log = rf.log[realIndex+1:]
	}
	rf.persist()
}

func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Delete(index)
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}
