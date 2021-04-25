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
const SendShardsGap = 500 * time.Millisecond
const DuplicateConfigCount = 10

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	return
}

func CriticalDPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug >= 1 {
		log.Printf("**"+format, a...)
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
	ShardNum   int
	ShardTS    int
	KvMap      map[string]string
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
	control          [shardmaster.NShards]bool  // persist
	shardTS          [shardmaster.NShards]int   // persist
	lastWaitingShard map[int64]int
	currentConfig    shardmaster.Config         // persist
	masterClerk      *shardmaster.Clerk
	shardCV          *sync.Cond
	waitingConfigIdx int
	duplicateCount   int
	firstGID         int                        // persist
	initialed        bool                       // persist
}

func (kv *ShardKV) GenerateGetResult(key string, reply *GetReply) {
	// Should lock and unlock outside
	_, _ = DPrintf("{%v:%v} Get (%v): Success", kv.me, kv.gid, key)
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
		e.Encode(kv.lastAppliedIndex) != nil ||
		e.Encode(kv.control) != nil ||
		e.Encode(kv.shardTS) != nil ||
		e.Encode(kv.currentConfig) != nil ||
		e.Encode(kv.firstGID) != nil ||
		e.Encode(kv.initialed) != nil{
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
	var control [shardmaster.NShards]bool
	var shardTS [shardmaster.NShards]int
	var currentConfig shardmaster.Config
	var firstGID int
	var initialed bool

	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&control) != nil ||
		d.Decode(&shardTS) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&firstGID) != nil ||
		d.Decode(&initialed) != nil {
		log.Fatal("Error while decode")
	} else {
		kv.kvMap = kvMap
		kv.lastAppliedIndex = lastAppliedIndex
		kv.control = control
		kv.shardTS = shardTS
		kv.currentConfig = currentConfig
		kv.firstGID = firstGID
		kv.initialed = initialed
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
	gid := kv.gid

	_, _ = DPrintf("{%v:%v} Get (%v)->[%v]: clerk:%v, index:%v",
		me, gid, key, shardNum, clerkId, index)

	if !isLeader {
		_, _ = DPrintf("{%v:%v} Get (%v): Not leader", me, gid, key)
		reply.Err = ErrWrongLeader
		return
	} else {
		_, _ = DPrintf("{%v:%v} Get (%v): Is leader", me, gid, key)
	}

	if kv.currentConfig.Shards[shardNum] != gid {
		_, _ = DPrintf("{%v:%v} Get (%v)->(%v): Wrong group", me, gid, key, shardNum)
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.control[shardNum] {
		_, _ = DPrintf("{%v:%v} Get (%v)->(%v): Need shards", me, gid, key, shardNum)
		reply.Err = ErrWaitShards
		return
	}
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index {
		_, _ = DPrintf("{%v:%v} Get (%v): Is trying or later request comes", me, gid, key)
		reply.Err = ErrOldRequest
		return
	}

	if kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
		kv.lastWaitingCV[clerkId] = nil
	}

	_, _ = DPrintf("{%v:%v} Get (%v): Add log", me, gid, key)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		Key: key, Value: "", OpString: OpStringGet}
	_, _, isLeader = kv.rf.Start(opCommand)

	if !isLeader {
		_, _ = DPrintf("{%v:%v} Get (%v): Not leader", me, gid, key)
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
		if kv.currentConfig.Shards[shardNum] != kv.gid {
			reply.Err = ErrWrongGroup
			_, _ = DPrintf("{%v:%v} Get (%v)->(%v): Wrong group-2", me, gid, key, shardNum)
			return
		}
		if !kv.control[shardNum] {
			reply.Err = ErrWaitShards
			_, _ = DPrintf("{%v:%v} Get (%v)->(%v): Need shards-2", me, gid, key, shardNum)
			return
		}

		_, _ = DPrintf("{%v:%v} Get (%v): Fail", me, gid, key)
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
	gid := kv.gid

	_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->[%v]: clerk:%v, index:%v",
		me, gid, key, value, shardNum, clerkId, index)

	if !isLeader {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Not leader",
			me, gid, key, value)
		reply.Err = ErrWrongLeader
		return
	} else {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Is leader",
			me, gid, key, value)
	}

	if kv.currentConfig.Shards[shardNum] != kv.gid {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->(%v): Wrong group",
			me, gid, key, value, shardNum)
		reply.Err = ErrWrongGroup
		return
	}
	if kv.control[shardNum] == false {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->(%v): Need shards",
			me, gid, key, value, shardNum)
		reply.Err = ErrWaitShards
		return
	}
	if kv.lastAppliedIndex[clerkId] >= index {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Already done",
			me, gid, key, value)
		reply.Err = OK
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Is trying or later request comes",
			me, gid, key, value)
		reply.Err = ErrOldRequest
		return
	}
	if kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
		kv.lastWaitingCV[clerkId] = nil
	}

	_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): will Start command",
		me, gid, key, value)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		Key: key, Value: value, OpString: args.Op}
	_, _, isLeader = kv.rf.Start(opCommand)

	if !isLeader {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Not leader",
			me, gid, key, value)
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
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Success",
			me, gid, key, value)
		reply.Err = OK
	} else {
		if kv.lastWaitingIndex[clerkId] == index {
			kv.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		if kv.currentConfig.Shards[shardNum] != kv.gid {
			reply.Err = ErrWrongGroup
			_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->(%v): Wrong group-2",
				me, gid, key, value, shardNum)
			return
		}
		if kv.currentConfig.Shards[shardNum] == kv.gid &&
			kv.control[shardNum] != true {
			reply.Err = ErrWaitShards
			_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->(%v): Need shards-2",
				me, gid, key, value, shardNum)
			return
		}

		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v): Fail",
			me, gid, key, value)
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

func (kv *ShardKV) logConfig(configToLog shardmaster.Config) {
	configOp := Op{
		ClerkId:    0,
		ClerkIndex: configToLog.Num,
		OpString:   OpStringConfig,
		Config:     configToLog}
	kv.rf.Start(configOp)
}

func (kv *ShardKV) SendOutShards() {
	// Should acquire lock outside the function
	for shardNum, gid := range kv.currentConfig.Shards {
		if kv.control[shardNum] && gid != kv.gid {
			kv.control[shardNum] = false
			go kv.SendShardRPC(shardNum, gid)
		}
	}
}

func (kv *ShardKV) SendShardRPC(shardNum int, gid int) {
	kv.mu.Lock()

	me := kv.me
	myGid := kv.gid

	args := SendShardArgs{
		ShardNum: shardNum,
		ShardTS:  kv.shardTS[shardNum] + 1, // +1 ! important!
		KvMap:    map[string]string{},
		Config:   kv.currentConfig,
	}
	for k, v := range kv.kvMap {
		if key2shard(k) == shardNum {
			args.KvMap[k] = v
		}
	}

	var servers []*labrpc.ClientEnd
	for _, name := range kv.currentConfig.Groups[gid] {
		servers = append(servers, kv.make_end(name))
	}

	kv.mu.Unlock()

	si := 0
	for {
		if si == len(servers) {
			si = 0
		}
		server := servers[si]

		var reply SendShardReply
		CriticalDPrintf("{%v:%v} will send [%v](TS:%v) to {%v}",
			me, myGid, shardNum, args.ShardTS, gid)
		ok := server.Call("ShardKV.SendShard", &args, &reply)

		if !ok {
			continue
		}
		if reply.Err == OK {
			CriticalDPrintf("{%v:%v} successfully send [%v](TS:%v) to {%v}",
				me, myGid, shardNum, args.ShardTS, gid)
			return
		}
		// reply.Err == ErrWrongLeader
		si++
		continue
	}

}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK

	if args.Config.Num > kv.currentConfig.Num {
		CriticalDPrintf("{%v:%v} log config: %v",
			kv.me, kv.gid, args.Config)
		kv.logConfig(args.Config)
	}

	shardNum := args.ShardNum
	if kv.shardTS[shardNum] >= args.ShardTS {
		return
	}

	CriticalDPrintf("{%v:%v} will log shard[%v](TS:%v)",
		kv.me, kv.gid, args.ShardNum, args.ShardTS)
	opCommand := Op{
		ShardNum: shardNum,
		KvMap:    map[string]string{},
		ShardTS:  args.ShardTS,
		OpString: OpStringKvMap,
	}

	for key, value := range args.KvMap {
		opCommand.KvMap[key] = value
	}

	_, _, isLeader = kv.rf.Start(opCommand)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Wait until the KV data is applied
	for {
		kv.shardCV.Wait()
		if kv.control[shardNum] {
			return
		}
	}

}

func (kv *ShardKV) SignalIfNewest(clerkId int64, idx int) {
	if kv.lastWaitingIndex[clerkId] == idx &&
		kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
		kv.lastWaitingCV[clerkId] = nil
	}
}

func (kv *ShardKV) TrySnapshot(commandIndex int) {
	if kv.maxraftstate > 0 &&
		kv.persister.RaftStateSize() > kv.maxraftstate*3/4 {
		_, _ = DPrintf("{%v:%v} will delete from %v", kv.me, kv.gid, commandIndex)
		ret := kv.rf.SaveSnapshot(commandIndex, kv.EncodeSnapShot())
		if ret {
			_, _ = DPrintf("{%v:%v} delete success, state: kvMap: %v, lastAppliedIndex: %v",
				kv.me, kv.gid, kv.kvMap, kv.lastAppliedIndex)
		} else {
			_, _ = DPrintf("{%v:%v} delete failed", kv.me, kv.gid)
		}
	}
}

func (kv *ShardKV) ControlDaemon() {
	kv.mu.Lock()
	_, _ = DPrintf("{%v:%v} begin!", kv.me, kv.gid)

	for {
		kv.mu.Unlock()

		applyMsg := <-kv.applyCh

		kv.mu.Lock()

		if applyMsg.Snapshot != nil {
			_, _ = DPrintf("{%v:%v} read snapshot", kv.me, kv.gid)
			kv.readSnapshot(applyMsg.Snapshot)
			_, _ = DPrintf("{%v:%v} state after install: kvMap: %v, lastAppliedIndex: %v",
				kv.me, kv.gid, kv.kvMap, kv.lastAppliedIndex)
			continue
		}

		_, _ = DPrintf("{%v:%v} apply log [%v]: %v",
			kv.me, kv.gid, applyMsg.CommandIndex, applyMsg.Command)

		if !applyMsg.IsLeader {
			for k, v := range kv.lastWaitingCV {
				if v != nil {
					v.Signal()
					kv.lastWaitingCV[k] = nil
				}
			}
		}

		op := applyMsg.Command.(Op)

		// TODO if KV, delete old and apply new
		if op.OpString == OpStringKvMap {
			if op.ShardTS <= kv.shardTS[op.ShardNum] {
				continue
			}

			CriticalDPrintf("{%v:%v} applied [%v](TS: %v)",
				kv.me, kv.gid, op.ShardNum, op.ShardTS)

			kv.control[op.ShardNum] = true
			kv.shardTS[op.ShardNum] = op.ShardTS
			for k := range kv.kvMap {
				if key2shard(k) == op.ShardNum {
					delete(kv.kvMap, k)
				}
			}
			for k, v := range op.KvMap {
				kv.kvMap[k] = v
			}

			kv.shardCV.Signal()
			if applyMsg.IsLeader {
				kv.SendOutShards()
			}

			kv.TrySnapshot(applyMsg.CommandIndex)
			continue
		}

		if op.OpString == OpStringConfig {

			if op.Config.Num <= kv.currentConfig.Num { // old or has applied
				continue
			}

			CriticalDPrintf("{%v:%v} apply config %v",
				kv.me, kv.gid, op.Config)
			kv.currentConfig = op.Config

			if kv.firstGID == kv.gid && !kv.initialed{
				kv.initialed = true
				// If some shard does not belong to me, I will send them out soon
				for i, _ := range kv.control{
					kv.control[i] = true
					kv.shardTS[i] = 1
				}
				CriticalDPrintf("{%v:%v} firstGID: %v, control: %v, shardTs: %v",
					kv.me, kv.gid, kv.firstGID, kv.control, kv.shardTS)
			}

			if applyMsg.IsLeader {
				kv.SendOutShards()
			}

			for k, v := range kv.lastWaitingShard {
				if kv.currentConfig.Shards[v] != kv.gid &&
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

		if kv.currentConfig.Shards[shardNum] == kv.gid &&
			kv.control[shardNum] {
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
	configRet := kv.masterClerk.Query(-1)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.firstGID < 0{
		kv.firstGID = kv.masterClerk.GetFirstGID()
	}
	_, isLeader := kv.rf.GetState()
	if isLeader && configRet.Num > kv.waitingConfigIdx {
		kv.duplicateCount = 0
		kv.waitingConfigIdx = configRet.Num
		kv.logConfig(configRet)
		CriticalDPrintf("{%v:%v} get config %v", kv.me, kv.gid, configRet)
		return
	}

	// 下面这段代码的意义等同于TimeOutRoutine,即写下Log之后失去了
	// Leader之位,未确认的Config被覆盖,结果不会重新给机会了
	if isLeader && configRet.Num == kv.waitingConfigIdx &&
		kv.currentConfig.Num < kv.waitingConfigIdx {
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

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.duplicateCount = 0
	kv.lastWaitingShard = map[int64]int{}
	kv.kvMap = map[string]string{}
	kv.lastAppliedIndex = map[int64]int{}
	kv.lastWaitingIndex = map[int64]int{}
	kv.lastWaitingCV = map[int64]*sync.Cond{}
	kv.persister = persister

	kv.currentConfig = shardmaster.Config{
		Num:    0,
		Shards: [10]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	kv.masterClerk = shardmaster.MakeClerk(masters)
	kv.shardCV = sync.NewCond(&kv.mu)
	kv.waitingConfigIdx = 0
	kv.duplicateCount = 0
	kv.firstGID = -1
	kv.initialed = false

	if kv.persister.SnapshotSize() > 0{
		kv.readSnapshot(kv.persister.ReadSnapshot())
		DPrintf("{%v:%v} read snapshot, kvMap: %v, control: %v, currentConfig: %v",
			kv.me, kv.gid, kv.kvMap, kv.control, kv.currentConfig)
	} else {
		DPrintf("{%v:%v} start!", kv.me, kv.gid)
	}

	go func() {
		newTimer := time.NewTimer(MasterQueryGap)
		for {
			kv.QueryMaster()
			newTimer.Reset(MasterQueryGap)
			<-newTimer.C
		}
	}()

	go func() {
		newTimer := time.NewTimer(SendShardsGap)
		for {
			newTimer.Reset(SendShardsGap)
			<-newTimer.C
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				kv.SendOutShards()
				kv.mu.Unlock()
			}
		}
	}()

	go kv.ControlDaemon()

	return kv
}
