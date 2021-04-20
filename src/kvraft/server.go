package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1
const TimeGap = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type LogId struct {
	clerkId    int64
	clerkIndex int
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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	kvMap            map[string]string // persist
	lastAppliedIndex map[int64]int     // persist
	lastWaitingIndex map[int64]int
	lastWaitingCV    map[int64]*sync.Cond
	persister        *raft.Persister
	//commitIndex     int
}

func (kv *KVServer) GenerateGetResult(key string, reply *GetReply) {
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

func TimeOutRoutine(cv *sync.Cond) {
	// 这里有点复杂要用中文...
	// 本来的设计是如果applyCh说我已经不是leader了，那么Signal()所有的
	// CV。但是有一种情况是:我写了几个log，新leader来了覆盖了这些log，但是
	// 还没有来得及apply我又当回了leader。这时候CV没有被提醒，[]waitingIndex
	// 没有被改变使得这个log明明没有在广播却也不能重新让leader广播。这时候就
	// 需要超时Signal()机制
	timer := time.NewTimer(2 * time.Second)
	<-timer.C
	cv.Signal()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here.
	_, isLeader := kv.rf.GetState()

	me := kv.me
	clerkId := args.ClerkId
	index := args.Index
	key := args.Key
	DPrintf("{%v} Get (%v): clerk:%v, index:%v",
		me, key, clerkId, index)

	if !isLeader {
		DPrintf("{%v} Get (%v): Not leader", me, key)
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("{%v} Get (%v): Is leader", me, key)
	}

	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index {
		DPrintf("{%v} Get (%v): Is trying or later request comes", me, key)
		reply.Err = ErrOldRequest
		return
	}
	if kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
	}
	kv.lastWaitingCV[clerkId] = nil
	kv.lastWaitingIndex[clerkId] = index

	DPrintf("{%v} Get (%v): add log", me, key)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		Key: key, Value: "", OpString: OpStringGet}
	_, _, _ = kv.rf.Start(opCommand)

	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
		return
	}
	if kv.lastWaitingIndex[clerkId] > index {
		DPrintf("{%v} Get (%v): later request comes", me, key)
		reply.Err = ErrOldRequest
		return
	}
	cv := sync.NewCond(&kv.mu)
	kv.lastWaitingCV[clerkId] = cv

	go TimeOutRoutine(cv)

	cv.Wait()

	reply.Value = ""
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
	} else {
		DPrintf("{%v} Get (%v): Fail", me, key)
		if kv.lastWaitingIndex[clerkId] == index {
			kv.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		reply.Err = ErrWrongLeader
	}

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here.
	_, isLeader := kv.rf.GetState()

	me := kv.me
	clerkId := args.ClerkId
	index := args.Index
	key := args.Key
	value := args.Value
	DPrintf("{%v} PutAppend (%v:%v): clerk:%v, index:%v",
		me, key, value, clerkId, index)

	if !isLeader {
		DPrintf("{%v} PutAppend (%v:%v): Not leader",
			me, key, value)
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("{%v} PutAppend (%v:%v): Is leader",
			me, key, value)
	}

	if kv.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} PutAppend (%v:%v): Already done",
			me, key, value)
		reply.Err = OK
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index {
		DPrintf("{%v} PutAppend (%v:%v): Is trying or later request comes",
			me, key, value)
		reply.Err = ErrOldRequest
		return
	}
	if kv.lastWaitingCV[clerkId] != nil {
		kv.lastWaitingCV[clerkId].Signal()
	}
	kv.lastWaitingCV[clerkId] = nil
	kv.lastWaitingIndex[clerkId] = index

	DPrintf("{%v} PutAppend (%v:%v): will Start command",
		me, key, value)
	opCommand := Op{ClerkId: clerkId, ClerkIndex: index,
		Key: key, Value: value, OpString: args.Op}
	_, _, _ = kv.rf.Start(opCommand)

	if kv.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} PutAppend (%v:%v): Success",
			me, key, value)
		reply.Err = OK
		return
	}
	if kv.lastWaitingIndex[clerkId] > index {
		DPrintf("{%v} PutAppend (%v:%v): Later request comes",
			me, key, value)
		reply.Err = ErrOldRequest
		return
	}

	cv := sync.NewCond(&kv.mu)
	kv.lastWaitingCV[clerkId] = cv

	go TimeOutRoutine(cv)

	cv.Wait()

	if kv.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} PutAppend (%v:%v): Success", me, key, value)
		reply.Err = OK
	} else {
		DPrintf("{%v} PutAppend (%v:%v): Fail", me, key, value)
		if kv.lastWaitingIndex[clerkId] == index {
			kv.lastWaitingIndex[clerkId] -= 1 // Now I'm failed and I'm not waiting
		}
		reply.Err = ErrWrongLeader
	}

	return
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start **goroutines**
// for any long-running work.
//

func (kv *KVServer) ControlDaemon() {
	kv.mu.Lock()
	_, _ = DPrintf("{%v} begin!", kv.me)

	for {
		kv.mu.Unlock()

		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh

		kv.mu.Lock()

		if applyMsg.Snapshot != nil {
			DPrintf("{%v} read snapshot", kv.me)
			kv.readSnapshot(applyMsg.Snapshot)
			DPrintf("{%v} state after install: kvMap: %v, lastAppliedIndex: %v",
				kv.me, kv.kvMap, kv.lastAppliedIndex)
			continue
		}

		DPrintf("{%v} apply log [%v]: %v", kv.me, applyMsg.CommandIndex, applyMsg.Command)

		if !applyMsg.IsLeader {
			for k, v := range kv.lastWaitingCV {
				if v != nil {
					v.Signal()
					kv.lastWaitingCV[k] = nil
				}
			}
		}

		op := applyMsg.Command.(Op)

		if kv.lastAppliedIndex[op.ClerkId] < op.ClerkIndex { // if is not duplicated log, apply it
			kv.lastAppliedIndex[op.ClerkId] = op.ClerkIndex
			switch op.OpString {
			case OpStringAppend:
				kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
			case OpStringPut:
				kv.kvMap[op.Key] = op.Value
			default:
			}
		}
		if kv.lastWaitingIndex[op.ClerkId] == op.ClerkIndex &&
			kv.lastWaitingCV[op.ClerkId] != nil {
			kv.lastWaitingCV[op.ClerkId].Signal()
			kv.lastWaitingCV[op.ClerkId] = nil
		}

		if kv.maxraftstate > 0 &&
			kv.persister.RaftStateSize() > kv.maxraftstate*3/4 {
			DPrintf("{%v} will delete from %v", kv.me, applyMsg.CommandIndex)
			ret := kv.rf.SaveSnapshot(applyMsg.CommandIndex, kv.EncodeSnapShot())
			if ret{
				DPrintf("{%v} delete success, state: kvMap: %v, lastAppliedIndex: %v",
					kv.me, kv.kvMap, kv.lastAppliedIndex)
			} else {
				DPrintf("{%v} delete failed", kv.me)
			}

		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.kvMap = map[string]string{}
	kv.lastWaitingCV = map[int64]*sync.Cond{}
	kv.lastWaitingIndex = map[int64]int{}
	kv.lastAppliedIndex = map[int64]int{}
	kv.readSnapshot(kv.persister.ReadSnapshot())
	DPrintf("{%v} start!", kv.me)

	go kv.ControlDaemon()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) EncodeSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.kvMap) != nil ||
		e.Encode(kv.lastAppliedIndex) != nil {
		log.Fatal("Error while encoding")
	}
	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var lastAppliedINdex map[int64]int

	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastAppliedINdex) != nil {
		log.Fatal("Error while decode")
	} else {
		kv.kvMap = kvMap
		kv.lastAppliedIndex = lastAppliedINdex
	}
}
