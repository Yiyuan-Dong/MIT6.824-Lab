package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const TryGap = 50 * time.Millisecond
const TimeWaiting = 500 * time.Millisecond

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId 	int64  // use a int64 random number to identify a clerk
	index   	int
	lastLeader  int
	mu          sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.index = 1

	ck.lastLeader = 0
	ck.mu = sync.Mutex{}
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.index++
	i := ck.lastLeader
	cv := sync.NewCond(&ck.mu)
	flag := false
	result := ""
	timer := time.NewTimer(TimeWaiting)
	_, _ = DPrintf("(%v) clerk get \"%v\" : first to server {%v}", ck.clerkId, key, ck.lastLeader)

	go func() {
		for {
			ck.mu.Lock()
			if flag {
				ck.mu.Unlock()
				return
			}
			timer.Reset(TimeWaiting)
			ck.mu.Unlock()

			<- timer.C

			ck.mu.Lock()
			cv.Signal()
			ck.mu.Unlock()
		}
	}()

	for {
		args := GetArgs{ClerkId: ck.clerkId, Key: key, Index: ck.index}
		reply := GetReply{}
		timer.Reset(TimeWaiting)

		go func(serverId int) {
			ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

			ck.mu.Lock()
			defer ck.mu.Unlock()

			if ok {
				switch reply.Err {
				case OK:
					flag = true
					result = reply.Value
					ck.lastLeader = serverId
					_, _ = DPrintf("(%v) clerk get \"%v\" : return \"%v\"",
						ck.clerkId, key, reply.Value)
				case ErrNoKey:
					flag = true
					result = ""
					ck.lastLeader = serverId
					_, _ = DPrintf("(%v) clerk get \"%v\" : return No KEY ERROR",
						ck.clerkId, key)
				case ErrOldRequest:
					ck.lastLeader = serverId
					_, _ = DPrintf("(%v) clerk get \"%v\" : is trying",
						ck.clerkId, key)
					return  // No need to cv.Signal(), we need to wait
				case ErrWrongLeader:
					_, _ = DPrintf("(%v) clerk get \"%v\" : Wrong leader",
						ck.clerkId, key)
				}
			} else {
				_, _ = DPrintf("(%v) clerk get \"%v\" : RPC failed",
					ck.clerkId, key)
			}

			cv.Signal()
		}(i)

		cv.Wait()

		if !flag{
			i = (i + 1) % len(ck.servers)
			//ck.index++   // For get, each log should be a new log
			_, _ = DPrintf("(%v) clerk get \"%v\" : turn to server {%v}", ck.clerkId, key, i)
			time.Sleep(TryGap)
			continue
		} else {
			return result
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.index++
	i := ck.lastLeader
	flag := false
	cv := sync.NewCond(&ck.mu)
	timer := time.NewTimer(TimeWaiting)
	_, _ = DPrintf("(%v) clerk %v (%v:%v) : first to server {%v}", ck.clerkId, op, key, value, ck.lastLeader)

	go func() {
		for {
			ck.mu.Lock()
			if flag {
				ck.mu.Unlock()
				return
			}
			timer.Reset(TimeWaiting)
			ck.mu.Unlock()

			<- timer.C

			ck.mu.Lock()
			cv.Signal()
			ck.mu.Unlock()
		}
	}()

	for {
		args := PutAppendArgs{ClerkId: ck.clerkId, Key: key, Index: ck.index, Value: value, Op: op}
		reply := PutAppendReply{}
		timer.Reset(TimeWaiting)

		go func(serverId int) {
			ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

			ck.mu.Lock()
			defer ck.mu.Unlock()

			if ok{
				switch reply.Err{
				case OK:
					flag = true
					ck.lastLeader = serverId
					_, _ = DPrintf("(%v) clerk %v (%v:%v) : Done!",
						ck.clerkId, op, key, value)
				case ErrOldRequest:
					ck.lastLeader = serverId
					_, _ = DPrintf("(%v) clerk %v (%v:%v) : Server is trying",
						ck.clerkId, op, key, value)
					return  // No need to cv.Signal(), we need to wait
				case ErrWrongLeader:
					_, _ = DPrintf("(%v) clerk %v (%v:%v) : Wrong leader",
						ck.clerkId, op, key, value)
				}
			} else {
				_, _ = DPrintf("(%v) clerk %v (%v:%v) : RPC failed!",
					ck.clerkId, op, key, value)
			}

			cv.Signal()
		}(i)

		cv.Wait()

		if !flag{
			i = (i + 1) % len(ck.servers)
			_, _ = DPrintf("(%v) clerk %v (%v:%v) : turn to server [%v]", ck.clerkId, op, key, value, i)
			time.Sleep(TryGap)
			continue
		} else {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpStringPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpStringAppend)
}
