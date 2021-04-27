package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

import "../shardmaster"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey#20000930"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOldRequest  = "ErrOldRequest"
	ErrWaitShards  = "ErrWaitShards"
	OpStringConfig = "Config"
	OpStringPut    = "Put"
	OpStringAppend = "Append"
	OpStringGet    = "Get"
	OpStringKvMap  = "KVMap"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId  int64
	Index    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId  int64
	Index    int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct{
	ShardNum         int
	ShardTS          int
	KvMap            map[string]string
	Config           shardmaster.Config
	FirstGID         int
	LastAppliedIndex map[int64]int
	LastGetAns       map[int64]string
}

type SendShardReply struct{
	Err Err
}
