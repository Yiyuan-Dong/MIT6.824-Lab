package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"../labrpc"
	"log"
	"sync"
)
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

const ConstWaitingGap = 300 * time.Millisecond
//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId  int64
	index    int
	mu       sync.Mutex
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.clerkId = nrand()
	ck.index = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	ck.mu.Lock()
	args.Index = ck.index
	ck.index++
	args.ClerkId = ck.clerkId
	ck.mu.Unlock()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			si := 0
			for {
				if si == len(servers){
					break
				}

				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("\"%v\" will now Get(), index: %v", ck.clerkId, ck.index)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				if ok && (reply.Err == ErrWaitShards) {
					time.Sleep(ConstWaitingGap)
					continue
				}
				if ok && (reply.Err == ErrOldRequest) {
					log.Fatal("WTF?")
				}

				// ... not ok, or ErrWrongLeader. Try other servers in the group
				si++
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		DPrintf("\"%v\" will Query() in Get(), index: %v", ck.clerkId, ck.index)
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	ck.mu.Lock()
	args.Index = ck.index
	ck.index++
	args.ClerkId = ck.clerkId
	ck.mu.Unlock()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			si := 0
			for {
				if si >= len(servers){
					break
				}
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("\"%v\" will PutAppend(), index: %v", ck.clerkId, ck.index)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				if ok && (reply.Err == ErrWaitShards) {
					time.Sleep(ConstWaitingGap)
					continue
				}
				if ok && (reply.Err == ErrOldRequest) {
					log.Fatal("WTF?")
				}

				// ... not ok, or ErrWrongLeader
				si++
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		DPrintf("\"%v\" will Query() in PutAppend(), index: %v", ck.clerkId, ck.index)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
