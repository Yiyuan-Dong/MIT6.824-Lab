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

	kvMap    		map[string]string
	logSet   		map[LogId]bool
	waitingCV  		map[LogId]*sync.Cond
	//commitIndex     int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("GET")
	_, isLeader := kv.rf.GetState()  // Take care! Need to avoid deadlock

	kv.mu.Lock()

	me := kv.me
	DPrintf("{%v} Get request \"%v\"", me, args.Key)

	if !isLeader{
		DPrintf("{%v} Get : Not leader", me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		DPrintf("{%v} Get : Is leader", me)
	}

	opCommand := Op{ClerkId:args.ClerkId, ClerkIndex: args.Index,
		Key: args.Key, Value: "", OpString: OpStringGet}
	logId := LogId{clerkIndex: args.Index, clerkId: args.ClerkId}

	_, ok := kv.logSet[logId]  // Read log has already kept in. directly return keeps linealizability
	if ok{
		DPrintf("{%v} Get : Already in", me)
		value, ok := kv.kvMap[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	}

	_, ok = kv.waitingCV[logId]

	kv.mu.Unlock()

	if !ok{
		DPrintf("{%v} Get : add log", me)
		_, _, _ = kv.rf.Start(opCommand)  // Take care! Need to avoid deadlock
	} else {
		DPrintf("{%v} Get : Is trying, plz wait", me)
		reply.Err = ErrTrying
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	cv := sync.NewCond(&kv.mu)
	kv.waitingCV[logId] = cv

	cv.Wait()

	_, ok = kv.logSet[logId]
	reply.Value = ""
	if ok {
		DPrintf("{%v} Get : Success", me)
		value, ok := kv.kvMap[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
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

	me := kv.me
	DPrintf("{%v} PutAppend: (%v:%v)", me, args.Key, args.Value)

	if !isLeader{
		DPrintf("{%v} PutAppend : Not leader", me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		DPrintf("{%v} PutAppend : Is leader", me)
	}

	opCommand := Op{ClerkId:args.ClerkId, ClerkIndex: args.Index,
		Key: args.Key, Value: args.Value, OpString: args.Op}
	logId := LogId{clerkIndex: args.Index, clerkId: args.ClerkId}

	_, ok := kv.logSet[logId]
	if ok{
		DPrintf("{%v} PutAppend : Already In", me)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	_, ok = kv.waitingCV[logId]

	kv.mu.Unlock()

	if !ok {
		DPrintf("{%v} PutAppend : Start command", me)
		_, _, _ = kv.rf.Start(opCommand)  // Take care! Need to avoid deadlock
	} else {
		DPrintf("{%v} PutAppend : Is trying, plz wait", me)
		reply.Err = ErrTrying
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	cv := sync.NewCond(&kv.mu)
	kv.waitingCV[logId] = cv

	cv.Wait()

	_, ok = kv.logSet[logId]
	if ok {
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

		if !applyMsg.IsLeader && len(kv.waitingCV) > 0{
			DPrintf("{%v} is no longer leader", kv.me)
			for _, v := range kv.waitingCV{
				v.Signal()
			}
			kv.waitingCV = map[LogId]*sync.Cond{}
		}

		op := applyMsg.Command.(Op)
		logId := LogId{clerkId: op.ClerkId, clerkIndex: op.ClerkIndex}

		_, ok := kv.logSet[logId]
		if !ok{  // if is not duplicated log, apply it
			kv.logSet[logId] = true
			switch op.OpString{
			case OpStringAppend:
				kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
			case OpStringPut:
				kv.kvMap[op.Key] = op.Value
			default:
			}
		}

		cv, ok := kv.waitingCV[logId]
		if ok{
			cv.Signal()
			delete(kv.waitingCV, logId)
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
	kv.logSet = map[LogId]bool{}
	kv.waitingCV = map[LogId]*sync.Cond{}

	go kv.ControlDaemon()
	// You may need initialization code here.

	return kv
}
