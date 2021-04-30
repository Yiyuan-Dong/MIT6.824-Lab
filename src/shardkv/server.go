/**
Dyy来介绍一些设计想法:
1.lastApplied:
	如果阅读代码会发现无论是Get()还是PutAppend()都有一个问题:
	比如当前某clerk最新的idx是7，而你的RPC的idx是6，那么你在
	wait()被唤醒时检查lastAppliedIndex==7会认为7>6，所以你认
	为请求被满足了。但是很有可能满足的是7，而6因为WrongGroup之
	类的原因没有被执行。lastGetAns同理。
	但这没有关系，因为clerk一次只执行一个请求，所以如果有idx更高
	的请求，那么之前的请求返回什么都没关系，因为clerk不再等他了。

2.control
	control代表了“控制权”的思想，他有两个特点:
	i.  任意时刻只有零个或一个group有某一shard的控制权。
	ii. 只有log的apply可以改变控制权。

3.SendOutLog
	有这样一个很严肃的问题:一旦一个Config被apply,那些原本属于我而如
	今不属于我的shard会被送出去并被删除。但问题是有可能我开始发送shard，
	还没送完，整个group就挂了，而且删除键值对之后的状态还被快照保存了。
	那么第一，重新启动后的系统不知道他有个被删掉的shard其实没有真正发出去;
	第二，就算他想重新发一遍，数据已经被删了。
	所以搞出了一个SendOutLog，思想是如果想要往外发一个shard，就在这里记
	下，如果发送成功了，再把它删掉。每次重启时检查有没有残存的Log。
 */

package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"bytes"
	"log"
	"sync/atomic"
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

func Max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	return
}

func CriticalDPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug >= 1 {
		log.Printf("*** "+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// (ClerkId, ClerkIndex) is used to uniquely identify an Op
	ClerkId          int64
	ClerkIndex       int
	Key              string
	Value            string
	OpString         string
	Config           shardmaster.Config
	ShardNum         int
	ShardTS          int
	KvMap            map[string]string
	LastAppliedIndex map[int64]int
	LastGetAns       map[int64]string
	FirstGID         int
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
	control          [shardmaster.NShards]bool // persist
	shardTS          [shardmaster.NShards]int  // persist
	lastWaitingShard map[int64]int
	currentConfig    shardmaster.Config // persist
	masterClerk      *shardmaster.Clerk
	shardCV          *sync.Cond
	waitingConfigIdx int
	duplicateCount   int
	firstGID         int  // persist
	initialized      bool // persist
	dead             int32
	lastGetAns       map[int64]string
	sendOutLogs      [shardmaster.NShards]SendOutLog  // persist

	raftTerm         int
}

type SendOutLog struct {
	GID       int
	TimeStamp int
	KvMap     map[string]string
}

func (kv *ShardKV) GenerateGetResult(key string, reply *GetReply, clerkId int64) {
	// Should lock and unlock outside
	value := kv.lastGetAns[clerkId]
	if value == ErrNoKey {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = value
	}
	_, _ = DPrintf("{%v:%v} Get (%v): Success, value: %v", kv.me, kv.gid, key, value)
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
		e.Encode(kv.initialized) != nil ||
		e.Encode(kv.sendOutLogs) != nil {
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
	var initialized bool
	var sendOutLogs [shardmaster.NShards]SendOutLog

	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&control) != nil ||
		d.Decode(&shardTS) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&firstGID) != nil ||
		d.Decode(&initialized) != nil ||
		d.Decode(&sendOutLogs) != nil {
		log.Fatal("Error while decode")
	} else {
		kv.kvMap = kvMap
		kv.lastAppliedIndex = lastAppliedIndex
		kv.control = control
		kv.shardTS = shardTS
		kv.currentConfig = currentConfig
		kv.firstGID = firstGID
		kv.initialized = initialized
		kv.sendOutLogs = sendOutLogs
	}
}

/**
  (现在我用CheckState()处理这种情况了)
  这里有点复杂要用中文...
  本来的设计是如果applyCh说我已经不是leader了，那么Signal()所有的
  CV。但是有一种情况是:我写了几个log，新leader来了覆盖了这些log，但是
  还没有来得及apply我又当回了leader。这时候CV没有被提醒，[]waitingIndex
  没有被改变使得这个log明明没有在广播却也不能重新让leader广播。这时候就
  需要超时Signal()机制
*/

//func TimeOutRoutine(cv *sync.Cond) {
//	timer := time.NewTimer(2 * time.Second)
//	<-timer.C
//	cv.Signal()
//}

func (kv *ShardKV) CheckState() bool {
	term, isLeader := kv.rf.GetState()
	if term > kv.raftTerm {
		kv.SignalWaitingCV()
		kv.raftTerm = term
	}
	return isLeader
}

func (kv *ShardKV)SignalWaitingCV(){
	for k, v := range kv.lastWaitingCV {
		if v != nil {
			v.Signal()
			kv.lastWaitingCV[k] = nil
		}
	}
	kv.shardCV.Signal()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here.
	isLeader := kv.CheckState()

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
		_, _ = DPrintf("{%v:%v} Get (%v)->[%v]: Wrong group", me, gid, key, shardNum)
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.control[shardNum] {
		_, _ = DPrintf("{%v:%v} Get (%v)->[%v]: Need shards", me, gid, key, shardNum)
		reply.Err = ErrWaitShards
		return
	}
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply, clerkId)
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

	cv.Wait()

	reply.Value = ""
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply, clerkId)
	} else {
		if kv.lastWaitingIndex[clerkId] == index {
			kv.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		if kv.currentConfig.Shards[shardNum] != kv.gid {
			reply.Err = ErrWrongGroup
			_, _ = DPrintf("{%v:%v} Get (%v)->[%v]: Wrong group-2", me, gid, key, shardNum)
			return
		}
		if !kv.control[shardNum] {
			reply.Err = ErrWaitShards
			_, _ = DPrintf("{%v:%v} Get (%v)->[%v]: Need shards-2", me, gid, key, shardNum)
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
	isLeader := kv.CheckState()

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
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->[%v]: Wrong group",
			me, gid, key, value, shardNum)
		reply.Err = ErrWrongGroup
		return
	}
	if kv.control[shardNum] == false {
		_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->[%v]: Need shards",
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
			_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->[%v]: Wrong group-2",
				me, gid, key, value, shardNum)
			return
		}
		if kv.currentConfig.Shards[shardNum] == kv.gid &&
			kv.control[shardNum] != true {
			reply.Err = ErrWaitShards
			_, _ = DPrintf("{%v:%v} PutAppend (%v:%v)->[%v]: Need shards-2",
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) logConfig(configToLog shardmaster.Config, firstGID int) {
	configOp := Op{
		ClerkId:    0,
		ClerkIndex: configToLog.Num,
		OpString:   OpStringConfig,
		Config:     configToLog,
		FirstGID:   firstGID,
	}
	_, _, isLeader := kv.rf.Start(configOp)
	if isLeader {
		CriticalDPrintf("{%v:%v} has log config %v, firstGID: %v",
			kv.me, kv.gid, configToLog, firstGID)
	}
}

func (kv *ShardKV) SendOutShards() {
	// Should acquire lock outside the function
	for shardNum, gid := range kv.currentConfig.Shards {
		if kv.control[shardNum] && gid != kv.gid {

			kv.control[shardNum] = false
			// Construct a SendOutLog
			kv.sendOutLogs[shardNum] = SendOutLog{
				GID:       gid,
				TimeStamp: kv.shardTS[shardNum],
				KvMap:     map[string]string{},
			}

			// Challenge 1: Delete
			kvMap := map[string]string{}
			for k, v := range kv.kvMap {
				if key2shard(k) == shardNum {
					kvMap[k] = v
					kv.sendOutLogs[shardNum].KvMap[k] = v
					delete(kv.kvMap, k)
					CriticalDPrintf("{%v:%v} key: %v, shardNum:[%v], KvMap: %v",
						kv.me, kv.gid, k, shardNum, kv.kvMap)
				}
			}

			var servers []*labrpc.ClientEnd
			for _, name := range kv.currentConfig.Groups[gid]{
				servers = append(servers, kv.make_end(name))
			}
			lastAppliedIndex := map[int64]int{}
			for k, v := range kv.lastAppliedIndex {
				lastAppliedIndex[k] = v
			}
			lastGetAns := map[int64]string{}
			for k, v := range kv.lastGetAns{
				lastGetAns[k] = v
			}

			args := SendShardArgs{
				ShardNum:         shardNum,
				ShardTS:          kv.shardTS[shardNum] + 1, // +1 ! important!
				KvMap:            kvMap,
				Config:           kv.currentConfig,
				LastAppliedIndex: lastAppliedIndex,
				FirstGID:         kv.firstGID,
				LastGetAns:       lastGetAns,
			}

			go kv.SendShardRPC(gid, args, servers)
		}
	}
}

func (kv *ShardKV) SendShardRPC(gid int, args SendShardArgs, servers []*labrpc.ClientEnd) {
	kv.mu.Lock()

	me := kv.me
	myGid := kv.gid

	kv.mu.Unlock()

	shardNum := args.ShardNum
	si := 0
	for {
		if kv.killed() {
			return
		}

		// If there are new servers for the group, we shall send to new servers
		if si == len(servers) {
			if si > 0 {
				time.Sleep(1000 * time.Millisecond)
			}

			si = 0

			var newServers []*labrpc.ClientEnd
			kv.mu.Lock()
			for _, name := range kv.currentConfig.Groups[gid] {
				newServers = append(newServers, kv.make_end(name))
			}
			kv.mu.Unlock()
			if len(newServers) == 0 {
				CriticalDPrintf("{%v:%v} what?", me, myGid)
				time.Sleep(300 * time.Millisecond)
				continue
			} else {
				servers = newServers
			}
		}

		server := servers[si]

		var reply SendShardReply
		CriticalDPrintf("{%v:%v} will send [%v](TS:%v) to {%v}(si:%v), kvMap: %v",
			me, myGid, shardNum, args.ShardTS, gid, si, args.KvMap)

		// 被抛弃的设计，现在是通过ApplyMsg.LogTerm判断leader的更替
		//
		// 有一种神秘的情况是某个server接受了我的shard，结果过了一会儿他不当
		// raft leader了，我的shard log也尸骨无存，这时候我不能傻等，不然就
		// 死循环了

		//tempCh := make(chan bool)
		//
		//go func() {
		//	timer := time.NewTimer(1000 * time.Millisecond)
		//	<- timer.C
		//	tempCh <- false
		//	CriticalDPrintf("DRAIN1")
		//}()
		//
		//go func() {
		//	callRet := server.Call("ShardKV.SendShard", &args, &reply)
		//	tempCh <- callRet
		//	CriticalDPrintf("DRAIN2")
		//}()
		//
		//ok := <- tempCh
		//go func() {
		//	// drain another goroutine
		//	<- tempCh
		//	CriticalDPrintf("Drain3")
		//}()

		ok := server.Call("ShardKV.SendShard", &args, &reply)

		if !ok {
			CriticalDPrintf("{%v:%v} sent [%v](TS:%v) to {%v}(si:%v), failed",
				me, myGid, shardNum, args.ShardTS, gid, si)
		} else {
			if reply.Err == OK {
				CriticalDPrintf("{%v:%v} sent [%v](TS:%v) to {%v}(si:%v), succeeded",
					me, myGid, shardNum, args.ShardTS, gid, si)

				// Delete SendOutLog
				kv.mu.Lock()
				if kv.sendOutLogs[shardNum].TimeStamp == args.ShardTS{
					kv.sendOutLogs[shardNum] = SendOutLog{
						GID:      -1,
						TimeStamp: 0,
					}
				}
				kv.mu.Unlock()

				return
			} else {
				CriticalDPrintf("{%v:%v} sent [%v](TS:%v) to {%v}(si:%v), Wrong leader",
					me, myGid, shardNum, args.ShardTS, gid, si)
			}
		}

		si++
		continue
	}

}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	isLeader := kv.CheckState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK

	if args.Config.Num > kv.currentConfig.Num {
		kv.logConfig(args.Config, args.FirstGID)
	}

	shardNum := args.ShardNum
	if kv.shardTS[shardNum] >= args.ShardTS {
		return
	}

	if kv.sendOutLogs[shardNum].TimeStamp < args.ShardTS{
		kv.sendOutLogs[shardNum] =
			SendOutLog{
				GID:      -1,
				TimeStamp: 0,
			}
	}

	CriticalDPrintf("{%v:%v} will log shard[%v](TS:%v), kvMap: %v",
		kv.me, kv.gid, args.ShardNum, args.ShardTS, args.KvMap)
	opCommand := Op{
		ShardNum:         shardNum,
		KvMap:            map[string]string{},
		ShardTS:          args.ShardTS,
		OpString:         OpStringKvMap,
		LastAppliedIndex: map[int64]int{},
		LastGetAns:       map[int64]string{},
	}

	for k, v := range args.KvMap {
		opCommand.KvMap[k] = v
	}
	for k, v := range args.LastAppliedIndex {
		opCommand.LastAppliedIndex[k] = v
	}
	for k, v := range args.LastGetAns {
		opCommand.LastGetAns[k] = v
	}

	_, _, isLeader = kv.rf.Start(opCommand)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	currentRaftTerm := kv.raftTerm

	// Wait until the KV data is applied or leader changed
	for {
		kv.shardCV.Wait()
		if kv.shardTS[shardNum] >= args.ShardTS {
			reply.Err = OK
			return
		}
		if kv.raftTerm > currentRaftTerm {
			reply.Err = ErrWrongLeader
			return
		}
	}

}

func (kv *ShardKV) SignalIfNewest(clerkId int64, idx int) {
	if kv.lastWaitingIndex[clerkId] <= idx &&
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
			_, _ = DPrintf("{%v:%v} snapshot succeeded, state: kvMap: %v, lastAppliedIndex: %v",
				kv.me, kv.gid, kv.kvMap, kv.lastAppliedIndex)
		} else {
			_, _ = DPrintf("{%v:%v} snapshot failed", kv.me, kv.gid)
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

		if applyMsg.LogTerm > kv.raftTerm {
			kv.SignalWaitingCV()
			kv.raftTerm = applyMsg.LogTerm
		}

		op := applyMsg.Command.(Op)

		// TODO if KV, delete old KV pair, and apply new KV pair
		if op.OpString == OpStringKvMap {
			if op.ShardTS <= kv.shardTS[op.ShardNum] {
				continue
			}

			CriticalDPrintf("{%v:%v} applied [%v](TS: %v), kvMap: %v",
				kv.me, kv.gid, op.ShardNum, op.ShardTS, op.KvMap)

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
			for k, v := range op.LastAppliedIndex {
				if v > kv.lastAppliedIndex[k] {
					kv.lastAppliedIndex[k] = v
					kv.lastGetAns[k] = op.LastGetAns[k]
				}
			}

			kv.shardCV.Signal()
			kv.SendOutShards()

			kv.TrySnapshot(applyMsg.CommandIndex)
			continue
		}

		if op.OpString == OpStringConfig {

			if op.Config.Num <= kv.currentConfig.Num { // old or has applied
				continue
			}

			CriticalDPrintf("{%v:%v} apply config %v, firstGID: %v",
				kv.me, kv.gid, op.Config, op.FirstGID)
			kv.currentConfig = op.Config

			if kv.firstGID < 0 {
				kv.firstGID = op.FirstGID
			}

			if kv.firstGID == kv.gid && !kv.initialized {
				kv.initialized = true
				// If some shard does not belong to me, I will send them out soon
				for i := range kv.control {
					kv.control[i] = true
					kv.shardTS[i] = 1
				}
				CriticalDPrintf("{%v:%v} firstGID: %v, control: %v, shardTS: %v",
					kv.me, kv.gid, kv.firstGID, kv.control, kv.shardTS)
			}

			kv.SendOutShards()

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
				case OpStringGet:
					DPrintf("{%v:%v}, key: %v, kvMap: %v",
						kv.me, kv.gid, op.Key, kv.kvMap)
					value, ok := kv.kvMap[op.Key]
					if !ok {
						kv.lastGetAns[op.ClerkId] = ErrNoKey
					} else {
						kv.lastGetAns[op.ClerkId] = value
					}
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

	if kv.firstGID < 0 {
		kv.firstGID = kv.masterClerk.GetFirstGID()
		CriticalDPrintf("{%v:%v} firstGID: %v, ConfigNum: %v",
			kv.me, kv.gid, kv.firstGID, configRet.Num)
	}

	if configRet.Num > kv.waitingConfigIdx {
		kv.duplicateCount = 0
		kv.waitingConfigIdx = configRet.Num
		kv.logConfig(configRet, kv.firstGID)
		return
	}

	// 下面这段代码的意义等同于TimeOutRoutine,即写下Log之后失去了
	// Leader之位,未确认的Config被覆盖,结果不会重新给机会了
	if configRet.Num == kv.waitingConfigIdx &&
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
	kv.initialized = false
	kv.lastGetAns = map[int64]string{}
	kv.raftTerm = -1

	if kv.persister.SnapshotSize() > 0 {
		kv.readSnapshot(kv.persister.ReadSnapshot())
		DPrintf("{%v:%v} read snapshot, kvMap: %v, control: %v, currentConfig: %v, sendOutLogs: %v",
			kv.me, kv.gid, kv.kvMap, kv.control, kv.currentConfig, kv.sendOutLogs)

		for shardNum, sendOutLog := range kv.sendOutLogs{
			if sendOutLog.TimeStamp == kv.shardTS[shardNum] &&
				sendOutLog.TimeStamp > 0{

				var servers []*labrpc.ClientEnd
				for _, name := range kv.currentConfig.Groups[sendOutLog.GID]{
					servers = append(servers, kv.make_end(name))
				}
				lastAppliedIndex := map[int64]int{}
				for k, v := range kv.lastAppliedIndex {
					lastAppliedIndex[k] = v
				}
				lastGetAns := map[int64]string{}
				for k, v := range kv.lastGetAns{
					lastGetAns[k] = v
				}
				kvMap := map[string]string{}
				for k, v := range sendOutLog.KvMap {
					kvMap[k] = v
				}

				args := SendShardArgs{
					ShardNum:         shardNum,
					ShardTS:          kv.shardTS[shardNum] + 1, // +1 ! important!
					KvMap:            kvMap,
					Config:           kv.currentConfig,
					LastAppliedIndex: lastAppliedIndex,
					FirstGID:         kv.firstGID,
					LastGetAns:       lastGetAns,
				}

				CriticalDPrintf("{%v:%v} resend [%v]", kv.me, kv.gid, shardNum)
				go kv.SendShardRPC(sendOutLog.GID, args, servers)
			}
		}

	} else {
		DPrintf("{%v:%v} start!", kv.me, kv.gid)
	}

	go func() {
		newTimer := time.NewTimer(MasterQueryGap)
		for {
			if kv.killed() {
				return
			}
			kv.QueryMaster()
			newTimer.Reset(MasterQueryGap)
			<-newTimer.C
		}
	}()

	// 因为每个Term最开始那个nil不会被apply。所以有可能Leader换了,
	// Log被覆盖了但是server一直不知道。这时候就要定时检查一下状态。
	go func() {
		newTimer := time.NewTimer(500 * time.Millisecond)
		for {
			<-newTimer.C

			if kv.killed() {
				return
			}
			newTimer.Reset(500 * time.Millisecond)
			kv.mu.Lock()
			kv.CheckState()
			kv.mu.Unlock()
		}
	}()

	go kv.ControlDaemon()

	return kv
}
