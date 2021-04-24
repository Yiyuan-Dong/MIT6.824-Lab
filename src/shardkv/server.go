package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"bytes"
	"log"
	"time"
)
import "../raft"
import "sync"
import "../labgob"
import "../shardmaster"

const Debug = 0
const MasterQueryGap = 100 * time.Millisecond
const DuplicateConfigCount = 10

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	return
}

func CriticalDPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// (ClerkId, ClerkIndex) is used to uniquely identify an Op
	ClerkId    int64
	ClerkIndex int
	Key        string
	Value      string
	OpString   string
	Config     shardmaster.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap            map[string]string // persist
	lastAppliedIndex map[int64]int     // persist
	lastWaitingIndex map[int64]int
	lastWaitingCV    map[int64]*sync.Cond
	persister        *raft.Persister

	// variable new added
	control          [shardmaster.NShards]bool
	shardTS          [shardmaster.NShards]int
	lastWaitingShard map[int64]int
	currentConfig    shardmaster.Config
	masterClerk      *shardmaster.Clerk
	configCV         *sync.Cond
	waitingConfigIdx int
	duplicateCount   int
}

func (kv *ShardKV) GenerateGetResult(key string, reply *GetReply) {
	// Should lock and unlock outside
	_, _ = DPrintf("{%v} Get (%v): Success", kv.me, key)
	value, ok := kv.kvMap[key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *ShardKV) EncodeSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.kvMap) != nil ||
		e.Encode(kv.lastAppliedIndex) != nil {
		log.Fatal("Error while encoding")
	}
	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var lastAppliedIndex map[int64]int

	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastAppliedIndex) != nil {
		log.Fatal("Error while decode")
	} else {
		kv.kvMap = kvMap
		kv.lastAppliedIndex = lastAppliedIndex
	}
}

func TimeOutRoutine(cv *sync.Cond) {
	// 这里有点复杂要用中文...
	// 本来的设计是如果applyCh说我已经不是leader了,那么Signal()所有的
	// CV.但是有一种情况是:我写了几个log,新leader来了覆盖了这些log,但是
	// 还没有来得及apply我又当回了leader.这时候CV没有被提醒,[]waitingIndex
	// 没有被改变使得这个log明明没有在广播却也不能重新让leader广播.这时候就
	// 需要超时Signal()机制
	timer := time.NewTimer(2 * time.Second)
	<-timer.C
	cv.Signal()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here.
	_, isLeader := kv.rf.GetState()

	me := kv.me
	clerkId := args.ClerkId
	index := args.Index
	key := args.Key
	shardNum := key2shard(key)

	_, _ = DPrintf("{%v} Get (%v)->(%v): clerk:%v, index:%v",
		me, key, shardNum, clerkId, index)

	if !isLeader {
		_, _ = DPrintf("{%v} Get (%v): Not leader", me, key)
		reply.Err = ErrWrongLeader
		return
	} else {
		_, _ = DPrintf("{%v} Get (%v): Is leader", me, key)
	}

	if kv.currentConfig.Shards[shardNum] != kv.me {
		_, _ = DPrintf("{%v} Get (%v)->(%v): Wrong group", me, key, shardNum)
		reply.Err = ErrWrongGroup
		return
	}
	if kv.control[shardNum] == false{
		_, _ = DPrintf("{%v} Get (%v)->(%v): Need shards", me, key, shardNum)
		reply.Err = ErrWaitShards
		return
	}
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index {
		_, _ = DPrintf("{%v} Get (%v): Is trying or later request comes", me, key)
		reply.Err = ErrOldRequest
		return
	}

	if kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
		kv.lastWaitingCV[clerkId] = nil
	}

	_, _ = DPrintf("{%v} Get (%v): add log", me, key)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		Key: key, Value: "", OpString: OpStringGet}
	_, _, isLeader = kv.rf.Start(opCommand)

	if !isLeader {
		_, _ = DPrintf("{%v} Get (%v): Not leader", me, key)
		reply.Err = ErrWrongLeader
		return
	}

	cv := sync.NewCond(&kv.mu)
	kv.lastWaitingIndex[clerkId] = index
	kv.lastWaitingCV[clerkId] = cv
	kv.lastWaitingShard[clerkId] = shardNum

	go TimeOutRoutine(cv)

	cv.Wait()

	reply.Value = ""
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
	} else {
		if kv.lastWaitingIndex[clerkId] == index {
			kv.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		if kv.currentConfig.Shards[shardNum] != kv.me{
			reply.Err = ErrWrongGroup
			_, _ = DPrintf("{%v} Get (%v)->(%v): wrong group", me, key, shardNum)
			return
		}
		if kv.currentConfig.Shards[shardNum] == kv.me &&
			kv.control[shardNum] != true{
			reply.Err = ErrWaitShards
			_, _ = DPrintf("{%v} Get (%v)->(%v): waits shards", me, key, shardNum)
			return
		}

		_, _ = DPrintf("{%v} Get (%v): Fail", me, key)
		reply.Err = ErrWrongLeader
	}

	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here.
	_, isLeader := kv.rf.GetState()

	me := kv.me
	clerkId := args.ClerkId
	index := args.Index
	key := args.Key
	value := args.Value
	shardNum := key2shard(key)

	_, _ = DPrintf("{%v} PutAppend (%v:%v)->(%v): clerk:%v, index:%v",
		me, key, value, shardNum, clerkId, index)

	if !isLeader {
		_, _ = DPrintf("{%v} PutAppend (%v:%v): Not leader",
			me, key, value)
		reply.Err = ErrWrongLeader
		return
	} else {
		_, _ = DPrintf("{%v} PutAppend (%v:%v): Is leader",
			me, key, value)
	}

	if kv.currentConfig.Shards[shardNum] != kv.me {
		_, _ = DPrintf("{%v} PutAppend (%v:%v)->(%v): Wrong group",
			me, key, value, shardNum)
		reply.Err = ErrWrongGroup
		return
	}
	if kv.control[shardNum] == false{
		_, _ = DPrintf("{%v} PutAppend (%v:%v)->(%v): Need shards",
			me, key, value, shardNum)
		reply.Err = ErrWaitShards
		return
	}
	if kv.lastAppliedIndex[clerkId] >= index {
		_, _ = DPrintf("{%v} PutAppend (%v:%v): Already done",
			me, key, value)
		reply.Err = OK
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index {
		_, _ = DPrintf("{%v} PutAppend (%v:%v): Is trying or later request comes",
			me, key, value)
		reply.Err = ErrOldRequest
		return
	}
	if kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
		kv.lastWaitingCV[clerkId] = nil
	}

	_, _ = DPrintf("{%v} PutAppend (%v:%v): will Start command",
		me, key, value)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		Key: key, Value: value, OpString: args.Op}
	_, _, isLeader = kv.rf.Start(opCommand)

	if !isLeader {
		_, _ = DPrintf("{%v} PutAppend (%v:%v): Not leader",
			me, key, value)
		reply.Err = ErrWrongLeader
		return
	}

	cv := sync.NewCond(&kv.mu)
	kv.lastWaitingIndex[clerkId] = index
	kv.lastWaitingCV[clerkId] = cv
	kv.lastWaitingShard[clerkId] = shardNum

	go TimeOutRoutine(cv)

	cv.Wait()

	if kv.lastAppliedIndex[clerkId] >= index {
		_, _ = DPrintf("{%v} PutAppend (%v:%v): Success", me, key, value)
		reply.Err = OK
	} else {
		if kv.lastWaitingIndex[clerkId] == index {
			kv.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		if kv.currentConfig.Shards[shardNum] != kv.me{
			reply.Err = ErrWrongGroup
			_, _ = DPrintf("{%v} PutAppend (%v:%v)->(%v): wrong group",
				me, key, value, shardNum)
			return
		}
		if kv.currentConfig.Shards[shardNum] == kv.me &&
			kv.control[shardNum] != true{
			reply.Err = ErrWaitShards
			_, _ = DPrintf("{%v} PutAppend (%v:%v)->(%v): waits shards",
				me, key, value, shardNum)
			return
		}

		_, _ = DPrintf("{%v} PutAppend (%v:%v): Fail", me, key, value)
		reply.Err = ErrWrongLeader
	}

	return
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) SendOutShards() {

	go kv.SendShardRPC()
}

func (kv *ShardKV) SendShardRPC() {

}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.configCV.Wait()

}

func (kv *ShardKV) SignalIfNewest(clerkId int64, idx int) {
	if kv.lastWaitingIndex[clerkId] == idx &&
		kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
		kv.lastWaitingCV[clerkId] = nil
	}
}

func (kv *ShardKV) TrySnapshot(commandIndex int){
	if kv.maxraftstate > 0 &&
		kv.persister.RaftStateSize() > kv.maxraftstate*3/4 {
		_, _ = DPrintf("{%v} will delete from %v", kv.me, commandIndex)
		ret := kv.rf.SaveSnapshot(commandIndex, kv.EncodeSnapShot())
		if ret {
			_, _ = DPrintf("{%v} delete success, state: kvMap: %v, lastAppliedIndex: %v",
				kv.me, kv.kvMap, kv.lastAppliedIndex)
		} else {
			_, _ = DPrintf("{%v} delete failed", kv.me)
		}
	}
}

func (kv *ShardKV) ControlDaemon() {
	kv.mu.Lock()
	_, _ = DPrintf("{%v} begin!", kv.me)

	for {
		kv.mu.Unlock()

		applyMsg := <-kv.applyCh

		kv.mu.Lock()

		if applyMsg.Snapshot != nil {
			_, _ = DPrintf("{%v} read snapshot", kv.me)
			kv.readSnapshot(applyMsg.Snapshot)
			_, _ = DPrintf("{%v} state after install: kvMap: %v, lastAppliedIndex: %v",
				kv.me, kv.kvMap, kv.lastAppliedIndex)
			continue
		}

		_, _ = DPrintf("{%v} apply log [%v]: %v",
			kv.me, applyMsg.CommandIndex, applyMsg.Command)

		if !applyMsg.IsLeader {
			for k, v := range kv.lastWaitingCV {
				if v != nil {
					v.Signal()
					kv.lastWaitingCV[k] = nil
				}
			}
		}

		op := applyMsg.Command.(Op)

		if op.OpString == OpStringConfig {
			if op.Config.Num <= kv.currentConfig.Num {
				continue
			}

			kv.currentConfig = op.Config

			if kv.currentConfig.Num == 1 {
				for i, v := range kv.currentConfig.Shards {
					if v == kv.me {
						kv.control[i] = true
						kv.shardTS[i] = 1
					}
				}
			}
			// TODO sendoutshards

			for k, v := range kv.lastWaitingShard{
				if kv.currentConfig.Shards[v] != kv.me &&
					kv.lastWaitingCV[k] != nil {
					kv.lastWaitingCV[k].Signal()
					kv.lastWaitingCV[k] = nil
				}
			}

			kv.TrySnapshot(applyMsg.CommandIndex)
			continue
		}

		kv.SignalIfNewest(op.ClerkId, op.ClerkIndex)
		shardNum := key2shard(op.Key)

		if kv.currentConfig.Shards[shardNum] == kv.me &&
			kv.control[shardNum]{
			if kv.lastAppliedIndex[op.ClerkId] < op.ClerkIndex {
				// if is not duplicated log, apply it
				kv.lastAppliedIndex[op.ClerkId] = op.ClerkIndex
				switch op.OpString {
				case OpStringAppend:
					kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
				case OpStringPut:
					kv.kvMap[op.Key] = op.Value
				default:
				}
			}
		}

		kv.TrySnapshot(applyMsg.CommandIndex)
	}
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (kv *ShardKV) QueryMaster() {
	config := kv.masterClerk.Query(-1)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if isLeader && config.Num > kv.waitingConfigIdx {
		kv.duplicateCount = 0
		kv.waitingConfigIdx = config.Num
		configOp := Op{
			ClerkId:    0,
			ClerkIndex: config.Num,
			OpString:   OpStringConfig,
			Config:     config}
		kv.rf.Start(configOp)

		return
	}

	// 下面这段代码的意义等同于TimeOutRoutine,即写下Log之后失去了
	// Leader之位,未确认的Config被覆盖,结果不会重新给机会了
	if isLeader && config.Num == kv.waitingConfigIdx {
		kv.duplicateCount++
		if kv.duplicateCount == DuplicateConfigCount {
			kv.duplicateCount = 0
			kv.waitingConfigIdx--
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.masterClerk = shardmaster.MakeClerk(masters)
	kv.configCV = sync.NewCond(&kv.mu)
	kv.duplicateCount = 0
	kv.lastWaitingShard = map[int64]int{}

	DPrintf("{%v} will begin!", kv.me)

	go func() {
		newTimer := time.NewTimer(MasterQueryGap)
		for {
			newTimer.Reset(MasterQueryGap)
			<-newTimer.C
			kv.QueryMaster()
		}
	}()

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
