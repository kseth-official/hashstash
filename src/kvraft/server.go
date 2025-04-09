package kvraft

import (
	"lab5/constants"
	"lab5/labgob"
	"lab5/labrpc"
	"lab5/logger"
	"lab5/raft"
	"sync"
	"sync/atomic"
	"time"
)

/*
Represents the valid operation types on the K/V server.
*/
const (
	GetOp uint8 = iota
	PutOp
	AppendOp
)

/*
Represents an operation that is appended to the raft log.
*/
type Op struct {
	// Operation type: 0 => Get, 1 => Put, 2 => Append
	Type uint8

	// Key for the operation
	Key string

	// Value (empty for Get)
	Val string

	// Client ID and sequence number for duplicate detection
	ClerkId int64
	SeqNum  int64
}

// NotifyChannel is used to notify client of operation completion
type NotifyChannel struct {
	// Channel to notify waiting client handlers
	ch chan OpResult

	// Identifies the operation
	clerkId int64
	seqNum  int64
}

// OpResult contains the result of an operation
type OpResult struct {
	// Error code
	Err Err

	// Value (for Get operations)
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	logger *logger.Logger

	// Key-value store
	kvStore map[string]string

	// Duplicate table maps from clerkId to the latest processed request
	dupTable map[int64]*RequestInfo

	// Channels to notify client handlers waiting for operation completion
	notifyChs map[int]NotifyChannel

	// Highest index in the log that has been applied
	lastApplied int
}

/*
Type used as value in the duplicate table.
*/
type RequestInfo struct {
	SeqNum int64
	Value  string
}

/*
RPC handler for Get calls to this K/V Server.
*/
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	// Create operation to submit to Raft
	op := Op{
		Type:    GetOp,
		Key:     args.Key,
		Val:     "",
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
	}

	// Submit operation to Raft log
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.logger.Log(constants.LogHandleGet, "Starting Get request from clerkId=%v, seqNum=%v, key=%v, index=%v",
		args.ClerkId, args.SeqNum, args.Key, index)

	// Check if this request has already been processed (duplicate detection)
	if info, exists := kv.dupTable[args.ClerkId]; exists && info.SeqNum == args.SeqNum {
		// This exact request has been processed, return cached result
		kv.logger.Log(constants.LogHandleGet, "Duplicate Get request detected from clerkId=%v, seqNum=%v", args.ClerkId, args.SeqNum)
		reply.Err = OK
		reply.Value = info.Value
		kv.mu.Unlock()
		return
	}

	// Create a notification channel for this operation
	ch := make(chan OpResult, 1)
	kv.notifyChs[index] = NotifyChannel{
		ch:      ch,
		clerkId: args.ClerkId,
		seqNum:  args.SeqNum,
	}
	kv.mu.Unlock()

	// Wait for operation to be applied (with timeout)
	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Value = result.Value
	case <-time.After(1000 * time.Millisecond):
		// If we time out, check if we're still the leader for the same term
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()

		currentTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || currentTerm != term {
			reply.Err = ErrWrongLeader
		} else {
			// Check if the operation was actually applied
			kv.mu.Lock()
			if info, exists := kv.dupTable[args.ClerkId]; exists && info.SeqNum >= args.SeqNum {
				reply.Err = OK
				if info.SeqNum == args.SeqNum {
					reply.Value = info.Value
				} else {
					reply.Value = ""
				}
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// Commenting out per Piazza @432
	//logger := logger.Logger(me)

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	//kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
