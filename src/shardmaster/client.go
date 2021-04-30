package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkId     int64  // use a int64 random number to identify a clerk
	index       int
	firstGID    int

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
	// Your code here.

	ck.clerkId = nrand()
	ck.index = 1
	ck.firstGID = -1

	return ck
}

func (ck *Clerk) GetFirstGID() int{
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return ck.firstGID
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Index = ck.index
	ck.index++
	args.ClerkId = ck.clerkId
	ck.mu.Unlock()

	DPrintf("'%v' Query() begin, index: %v", ck.clerkId, ck.index)

	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			//DPrintf("'%v' will Query(), index: %v", ck.clerkId, ck.index)
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.firstGID = reply.FirstGID
				ck.mu.Unlock()
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Index = ck.index
	ck.index++
	args.ClerkId = ck.clerkId
	ck.mu.Unlock()

	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Index = ck.index
	ck.index++
	args.ClerkId = ck.clerkId
	ck.mu.Unlock()

	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Index = ck.index
	ck.index++
	args.ClerkId = ck.clerkId
	ck.mu.Unlock()

	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
