package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const TimeGap = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type LogId struct {
	clerkId     	int64
	clerkIndex      int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId     	int64
	ClerkIndex      int
	Key         	string
	Value       	string
	OpString    	string
}

type KVServer struct {
	mu      		sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	dead    		int32 // set by Kill()

	maxraftstate 	int // snapshot if log grows this big

	// Your definitions here.

	kvMap    		map[string]string  // persist
	lastAppliedIndex   map[int64]int   // persist
	lastWaitingIndex   map[int64]int
	lastWaitingCV      map[int64]*sync.Cond
	//commitIndex     int
}

func (kv *KVServer) GenerateGetResult(key string, reply *GetReply){
	// Should lock and unlock outside
	_, _ = DPrintf("{%v} Get : Success", kv.me)
	value, ok := kv.kvMap[key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("GET")
	_, isLeader := kv.rf.GetState()  // Take care! Need to avoid deadlock

	kv.mu.Lock()
	defer kv.mu.Unlock()

	me := kv.me
	clerkId := args.ClerkId
	index := args.Index
	key := args.Key
	DPrintf("{%v} Get request \"%v\"", me, key)

	if !isLeader{
		DPrintf("{%v} Get : Not leader", me)
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("{%v} Get : Is leader", me)
	}

	if kv.lastAppliedIndex[clerkId] >= args.Index{
		kv.GenerateGetResult(key, reply)
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index{
		DPrintf("{%v} Get : Is trying or later request comes", me)
		reply.Err = ErrOldRequest
		return
	}
	if kv.lastWaitingCV[args.ClerkId] != nil{
		kv.lastWaitingCV[args.ClerkId].Signal()
	}
	kv.lastWaitingCV[args.ClerkId] = nil
	kv.lastWaitingIndex[args.ClerkId] = args.Index

	kv.mu.Unlock()

	DPrintf("{%v} Get : add log", me)
	opCommand := Op{ClerkId:clerkId, ClerkIndex: index,
		Key: args.Key, Value: "", OpString: OpStringGet}
	_, _, _ = kv.rf.Start(opCommand)  // Take care! Need to avoid deadlock

	kv.mu.Lock()

	if kv.lastAppliedIndex[clerkId] >= index{
		kv.GenerateGetResult(args.Key, reply)
		return
	}
	if kv.lastWaitingIndex[clerkId] > index{
		DPrintf("{%v} Get : later request comes", me)
		reply.Err = ErrOldRequest
		return
	}
	cv := sync.NewCond(&kv.mu)
	kv.lastWaitingCV[args.ClerkId] = cv

	cv.Wait()

	reply.Value = ""
	if kv.lastAppliedIndex[clerkId] >= index {
		kv.GenerateGetResult(key, reply)
	} else {
		DPrintf("{%v} Get : Fail", me)
		reply.Err = ErrWrongLeader
	}

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("PUTAPPEND")
	_, isLeader := kv.rf.GetState()  // Take care! Need to avoid deadlock

	kv.mu.Lock()
	defer kv.mu.Unlock()

	me := kv.me
	clerkId := args.ClerkId
	index := args.Index
	key := args.Key
	value := args.Value
	DPrintf("{%v} PutAppend: (%v:%v)", me, args.Key, args.Value)

	if !isLeader{
		DPrintf("{%v} PutAppend : Not leader", me)
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("{%v} PutAppend : Is leader", me)
	}

	if kv.lastAppliedIndex[clerkId] >= index{
		DPrintf("{%v} PutAppend : Already done", me)
		reply.Err = OK
		return
	}
	if kv.lastWaitingIndex[clerkId] >= index{
		DPrintf("{%v} Get : Is trying or later request comes", me)
		reply.Err = ErrOldRequest
		return
	}
	if kv.lastWaitingCV[args.ClerkId] != nil{
		kv.lastWaitingCV[args.ClerkId].Signal()
	}
	kv.lastWaitingCV[args.ClerkId] = nil
	kv.lastWaitingIndex[args.ClerkId] = args.Index

	kv.mu.Unlock()

	DPrintf("{%v} PutAppend : Start command", me)
	opCommand := Op{ClerkId:clerkId, ClerkIndex: index,
		Key: key, Value: value, OpString: args.Op}
	_, _, _ = kv.rf.Start(opCommand)  // Take care! Need to avoid deadlock

	kv.mu.Lock()

	if kv.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} PutAppend : Success", me)
		reply.Err = OK
		return
	}
	if kv.lastWaitingIndex[clerkId] > index {
		DPrintf("{%v} PutAppend : Later request comes", me)
		reply.Err = ErrOldRequest
		return
	}

	cv := sync.NewCond(&kv.mu)
	kv.lastWaitingCV[clerkId] = cv

	cv.Wait()

	if kv.lastAppliedIndex[clerkId] >= index {
		DPrintf("{%v} PutAppend : Success", me)
		reply.Err = OK
	} else {
		DPrintf("{%v} PutAppend : Fail", me)
		reply.Err = ErrWrongLeader
	}

	return
}

//
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

func (kv *KVServer) ControlDaemon(){
	kv.mu.Lock()
	_, _ = DPrintf("{%v} begin!", kv.me)

	for {
		kv.mu.Unlock()
		if kv.killed(){
			_, _ = DPrintf("Bye~~")
			return
		}

		applyMsg := <-kv.applyCh

		kv.mu.Lock()

		DPrintf("{%v} apply log [%v] : %v", kv.me, applyMsg.CommandIndex, applyMsg.Command)

		if !applyMsg.IsLeader {
			for k, v := range kv.lastWaitingCV{
				if v != nil{
					v.Signal()
					kv.lastWaitingCV[k] = nil
				}
			}
		}

		op := applyMsg.Command.(Op)

		if kv.lastAppliedIndex[op.ClerkId] < op.ClerkIndex{  // if is not duplicated log, apply it
			kv.lastAppliedIndex[op.ClerkId] = op.ClerkIndex
			switch op.OpString{
			case OpStringAppend:
				kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
			case OpStringPut:
				kv.kvMap[op.Key] = op.Value
			default:
			}
		}
		if kv.lastWaitingIndex[op.ClerkId] == op.ClerkIndex &&
			kv.lastWaitingCV[op.ClerkId] != nil{
			kv.lastWaitingCV[op.ClerkId].Signal()
			kv.lastWaitingCV[op.ClerkId] = nil
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

	kv.kvMap = map[string]string{}
	kv.lastWaitingCV = map[int64]*sync.Cond{}
	kv.lastWaitingIndex = map[int64]int{}
	kv.lastAppliedIndex = map[int64]int{}

	go kv.ControlDaemon()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) persist(){

}

