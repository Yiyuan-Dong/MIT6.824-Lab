package shardmaster

import (
	"../raft"
	"fmt"
	"log"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var size = [NShards][]int{
	{10},
	{5, 5},
	{4, 3, 3},
	{3, 3, 2, 2},
	{2, 2, 2, 2, 2},
	{2, 2, 2, 2, 1, 1},
	{2, 2, 2, 1, 1, 1, 1},
	{2, 2, 1, 1, 1, 1, 1, 1},
	{2, 1, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs 		 []Config // indexed by config num
	configsCount     int      // a sum, not an index! sum == lastIndex + 1
	order            map[int]int

	lastAppliedIndex map[int64]int     // persist
	lastWaitingIndex map[int64]int
	lastWaitingCV    map[int64]*sync.Cond
	persister        *raft.Persister
}


type Op struct {
	// Your data here.
	ClerkId     int64
	ClerkIndex  int
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveGID     int
	MoveShard   int
	QueryNumber int
	OpString    string
}

func MIN(x int, y int) int{
	if x < y{
		return x
	} else {
		return y
	}
}

func Max(x int, y int) int{
	if x > y{
		return x
	} else {
		return y
	}
}

func TimeOutRoutine(cv *sync.Cond) {
	// 这里有点复杂要用中文...
	// 本来的设计是如果applyCh说我已经不是leader了，那么Signal()所有的
	// CV。但是有一种情况是:我写了几个log，新leader来了覆盖了这些log，但是
	// 还没有来得及apply我又当回了leader。这时候CV没有被提醒，需要用超时
	// Signal()去提醒
	timer := time.NewTimer(2 * time.Second)
	<-timer.C
	cv.Signal()
}

func (sm *ShardMaster) Solve(clerkId int64, index int,
	logString string, opCommand Op) Err {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, isLeader := sm.rf.GetState()

	me := sm.me

	DPrintf("{%v} %v: clerk:%v, index:%v",
		me, logString, clerkId, index)

	if !isLeader {
		DPrintf("{%v} %v: Not leader", me, logString)
		return ErrWrongLeader
	} else {
		DPrintf("{%v} %v: Is leader", me, logString)
	}

	if sm.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} %v: Already done", me, logString)
		return OK
	}
	if sm.lastWaitingIndex[clerkId] >= index {
		DPrintf("{%v} %v: Is trying or later request comes",
			me, logString)
		return ErrOldRequest
	}
	if sm.lastWaitingCV[clerkId] != nil {
		sm.lastWaitingCV[clerkId].Signal()
	}
	sm.lastWaitingCV[clerkId] = nil
	sm.lastWaitingIndex[clerkId] = index

	DPrintf("{%v} %v: will Start command", me, logString)
	_, _, _ = sm.rf.Start(opCommand)

	if sm.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} %v: Success", me, logString)
		return OK
	}
	if sm.lastWaitingIndex[clerkId] > index {
		DPrintf("{%v} %v: Later request comes", me, logString)
		return ErrOldRequest
	}

	cv := sync.NewCond(&sm.mu)
	sm.lastWaitingCV[clerkId] = cv

	go TimeOutRoutine(cv)

	cv.Wait()

	if sm.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} %v: Success", me, logString)
		return OK
	} else {
		DPrintf("{%v} %v: Fail", me, logString)
		if sm.lastWaitingIndex[clerkId] == index {
			// Now I'm failed and I'm not waiting
			sm.lastWaitingIndex[clerkId] -= 1
		}
		return ErrWrongLeader
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply){
	opCommand := Op{
		ClerkId: args.ClerkId,
		ClerkIndex: args.Index,
		JoinServers: args.Servers,
		OpString: OpStringJoin,
	}

	ret := sm.Solve(
		args.ClerkId,
		args.Index,
		fmt.Sprintf("Join (%v)", args.Servers),
		opCommand,
	)

	reply.Err = ret
	reply.WrongLeader = ret != OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	opCommand := Op{
		ClerkId:    args.ClerkId,
		ClerkIndex: args.Index,
		LeaveGIDs:  args.GIDs,
		OpString:   OpStringLeave,
	}

	ret := sm.Solve(
		args.ClerkId,
		args.Index,
		fmt.Sprintf("Leave (%v)", args.GIDs),
		opCommand,
	)

	reply.Err = ret
	reply.WrongLeader = ret != OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	opCommand := Op{
		ClerkId:    args.ClerkId,
		ClerkIndex: args.Index,
		MoveGID: args.GID,
		MoveShard: args.Shard,
		OpString:   OpStringMove,
	}

	ret := sm.Solve(
		args.ClerkId,
		args.Index,
		fmt.Sprintf("Move (%v -> %v)", args.Shard, args.GID),
		opCommand,
	)

	reply.Err = ret
	reply.WrongLeader = ret != OK
}

func (sm *ShardMaster) GenerateQueryResult(queryNum int, reply *QueryReply){
	if queryNum < 0 || queryNum + 1 > sm.configsCount{
		reply.Config = sm.configs[sm.configsCount - 1]
	} else {
		reply.Config = sm.configs[queryNum]
	}
	reply.WrongLeader = false
	DPrintf("{%v} Query (%v): Succeed, config: %v", sm.me, queryNum, reply.Config)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	reply.WrongLeader = true

	// Your code here.
	_, isLeader := sm.rf.GetState()

	me := sm.me
	clerkId := args.ClerkId
	index := args.Index
	queryNum := args.Num
	DPrintf("{%v} Query (%v): clerk:%v, index:%v",
		me, queryNum, clerkId, index)

	if !isLeader {
		DPrintf("{%v} Query (%v): Not leader", me, queryNum)
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("{%v} Query (%v): Is leader", me, queryNum)
	}

	if sm.lastAppliedIndex[clerkId] >= index {
		sm.GenerateQueryResult(queryNum, reply)
		return
	}
	if sm.lastWaitingIndex[clerkId] >= index {
		DPrintf("{%v} Query (%v): Is trying or later request comes", me, queryNum)
		reply.Err = ErrOldRequest
		return
	}
	if sm.lastWaitingCV[clerkId] != nil {
		sm.lastWaitingCV[clerkId].Signal()
	}
	sm.lastWaitingCV[clerkId] = nil
	sm.lastWaitingIndex[clerkId] = index

	DPrintf("{%v} Query (%v): add log", me, queryNum)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		QueryNumber: queryNum, OpString: OpStringQuery}
	_, _, _ = sm.rf.Start(opCommand)

	if sm.lastAppliedIndex[clerkId] >= index {
		sm.GenerateQueryResult(queryNum, reply)
		return
	}
	if sm.lastWaitingIndex[clerkId] > index {
		DPrintf("{%v} Query (%v): later request comes", me, queryNum)
		reply.Err = ErrOldRequest
		return
	}
	cv := sync.NewCond(&sm.mu)
	sm.lastWaitingCV[args.ClerkId] = cv

	go TimeOutRoutine(cv)

	cv.Wait()

	if sm.lastAppliedIndex[clerkId] >= index {
		sm.GenerateQueryResult(queryNum, reply)
	} else {
		DPrintf("{%v} Query (%v): Fail", me, queryNum)
		if sm.lastWaitingIndex[clerkId] == index {
			sm.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		reply.Err = ErrWrongLeader
	}

	return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) CopyLastConfig() {
	lastIndex := sm.configsCount - 1
	newConfig := Config{
		Num: sm.configsCount,
		Shards: [NShards]int{},
		Groups: map[int][]string{}}
	sm.configsCount++

	for i, v := range sm.configs[lastIndex].Shards{
		newConfig.Shards[i] = v
	}
	for k, group := range sm.configs[lastIndex].Groups{
		for _, v := range group{
			newConfig.Groups[k] = append(newConfig.Groups[k], v)
		}
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) JoinOne(joinGID int){ // Modify Shards
	sm.order[joinGID] = len(sm.order)

	allocate := map[int][]int{}
	lastConfig := &sm.configs[sm.configsCount - 1]

	if len(sm.order) == 1{
		for i, _ := range lastConfig.Shards{
			lastConfig.Shards[i] = joinGID
		}
		return
	}

	for i, v := range lastConfig.Shards{
		allocate[v] = append(allocate[v], i)
	}

	for k, v := range allocate{
		sizeDeserve := 0
		if sm.order[k] >= 10{
			sizeDeserve = 0
		} else {
			sizeDeserve = size[MIN(len(sm.order) - 1, 9)][sm.order[k]]
		}
		reallocateCount := len(v) - sizeDeserve
		for _, shard := range v[:reallocateCount]{
			lastConfig.Shards[shard] = joinGID
		}
	}
}

func (sm *ShardMaster) LeaveOne(leaveGID int){ // Modify Shards
	hisOrder := sm.order[leaveGID]
	for k, v := range sm.order{
		if v > hisOrder{
			sm.order[k] -= 1
		}
	}
	delete(sm.order, leaveGID)

	allocate := map[int][]int{}
	lastConfig := &sm.configs[sm.configsCount - 1]
	for i, v := range lastConfig.Shards{
		allocate[v] = append(allocate[v], i)
	}
	for k, v := range sm.order{
		if v <= 9{
			_, ok := allocate[k]
			if !ok{
				allocate[k] = []int{}
			}
		}
	}


	for k, v := range allocate{
		if k == leaveGID{
			continue
		}

		sizeDeserve := 0
		if sm.order[k] >= 10{
			sizeDeserve = 0
		} else {
			sizeDeserve = size[MIN(len(sm.order) - 1, 9)][sm.order[k]]
		}
		reallocateCount := sizeDeserve - len(v)
		for i := 0; i < reallocateCount; i++{
			lastConfig.Shards[allocate[leaveGID][0]] = k
			allocate[leaveGID] = allocate[leaveGID][1:]
		}
	}
}

func (sm *ShardMaster) ControlDaemon() {
	sm.mu.Lock()
	_, _ = DPrintf("{%v} begin!", sm.me)

	for {
		sm.mu.Unlock()

		applyMsg := <-sm.applyCh

		sm.mu.Lock()

		// applyMsg will not contain snapshot

		DPrintf("{%v} apply log [%v]: %v", sm.me, applyMsg.CommandIndex, applyMsg.Command)

		if !applyMsg.IsLeader {
			for k, v := range sm.lastWaitingCV {
				if v != nil {
					v.Signal()
					sm.lastWaitingCV[k] = nil
				}
			}
		}

		op := applyMsg.Command.(Op)

		if sm.lastAppliedIndex[op.ClerkId] < op.ClerkIndex { // if is not duplicated log, apply it
			sm.lastAppliedIndex[op.ClerkId] = op.ClerkIndex
			switch op.OpString {
			case OpStringLeave:
				sm.CopyLastConfig()
				for _, v := range op.LeaveGIDs{
					sm.LeaveOne(v)
					delete(sm.configs[sm.configsCount - 1].Groups, v)
					DPrintf("{%v} leave %v, config: %v, order: %v",
						sm.me, v, sm.configs[sm.configsCount - 1], sm.order)
				}
			case OpStringJoin:
				sm.CopyLastConfig()
				for k, v := range op.JoinServers{
					sm.JoinOne(k)
					sm.configs[sm.configsCount - 1].Groups[k] = v
					DPrintf("{%v} join %v, config: %v, order: %v",
						sm.me, k, sm.configs[sm.configsCount - 1], sm.order)
				}
			case OpStringMove:
				sm.CopyLastConfig()
				sm.configs[sm.configsCount - 1].Shards[op.MoveShard] = op.MoveGID
			default:
			}
		}
		if sm.lastWaitingIndex[op.ClerkId] == op.ClerkIndex &&
			sm.lastWaitingCV[op.ClerkId] != nil {
			sm.lastWaitingCV[op.ClerkId].Signal()
			sm.lastWaitingCV[op.ClerkId] = nil
		}

		// Do not need to delete logs and install snapshot
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.configsCount = 1
	sm.order = map[int]int{}
	sm.lastAppliedIndex = map[int64]int{}
	sm.lastWaitingIndex = map[int64]int{}
	sm.lastWaitingCV = map[int64]*sync.Cond{}
	sm.persister = persister

	// Your code here.
	go sm.ControlDaemon()

	return sm
}
