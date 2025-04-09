package kvraft

import (
	"crypto/rand"
	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
	"math/big"
)

const ClerkIdMax int64 = int64(1) << 62

/*
Choosing max seq number such that about 2^32 RPCs could still go
go through in the worst case for 1 client. Probably needs more analysis
here of what the optimal ceiling would be for least collisions and not
overrunning the int64 size limit, but this should be okay for now.
*/
const SeqNumMax int64 = int64(1) << 32

/*
MAGIC STRINGS FOR RPC CALLS
*/
const (
	GetRPC       = "KVServer.Get"
	PutAppendRPC = "KVServer.PutAppend"
)

/*
For my sanity:

Each k/v server has a 1-to-1 connection with an associated raft peer.
One of these raft peer's and therefore, the associated k/v server is the
leader.
*/

type Clerk struct {
	servers []*labrpc.ClientEnd
	logger  *logger.Logger

	// clerkId is a unique identifier for the Clerk instance, used to distinguish between different clients.
	clerkId int64

	// seqNum is a monotonically increasing sequence number used to uniquely identify client requests.
	seqNum int64

	/*
		lastKnownLeader tracks the index of the most recent k/v server who's associated raft node was the leader.
		allows directly sending to the last known leader server once a leader has been discovered
		since the k/v server the rpc is sent to will respond with ErrWrongLeader if it is not associated with
		the raft leader.
	*/
	lastKnownLeader int
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
	// You'll have to add code here.

	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

/*
Abstraction to send the PutAppend request and receive the reply by reference.
*/
func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call(PutAppend, &args, &reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
