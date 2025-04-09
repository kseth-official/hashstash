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

/*
nrand generates a random int64 within the range [0, upperLimit). It uses cryptographic randomness for secure generation.
*/
func nrand(upperLimit int64) int64 {
	max := big.NewInt(upperLimit)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// Assign a randomized clerkId from [0, ..., ClerkIdMax)
	ck.clerkId = nrand(ClerkIdMax)
	// Assign a randomized starting seqNum from [0, ..., SeqNumMax)
	ck.seqNum = nrand(SeqNumMax)

	// Start with a random server
	ck.lastKnownLeader = int(nrand(int64(len(ck.servers))))

	// Casting back to integer here for clerkId. Might need to deal with this detail later.
	ck.logger = logger.NewLogger(int(ck.clerkId), false, "Clerk", constants.ClerkLoggingMap)

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
	// Increment sequence number for this request
	ck.seqNum++
	currentSeqNum := ck.seqNum

	args := GetArgs{
		Key:     key,
		ClerkId: ck.clerkId,
		SeqNum:  currentSeqNum,
	}

	ck.logger.Log(constants.LogGet, "Sending Get request for key=%v, seqNum=%v", key, currentSeqNum)

	// Keep trying until we get a successful response
	for {
		// Try the last known leader first
		server := ck.lastKnownLeader

		for i := 0; i < len(ck.servers); i++ {
			reply := GetReply{}

			ck.logger.Log(constants.LogGet, "Trying server %d for Get request seqNum=%v", server, currentSeqNum)

			// Send the RPC to the server
			ok := ck.servers[server].Call(GetRPC, &args, &reply)

			if ok && reply.Err == OK {
				// Successfully processed request
				ck.lastKnownLeader = server
				ck.logger.Log(constants.LogGet, "Get request successful for key=%v, seqNum=%v, value=%v", key, currentSeqNum, reply.Value)
				return reply.Value
			}

			if ok && reply.Err == ErrNoKey {
				// Key doesn't exist
				ck.lastKnownLeader = server
				ck.logger.Log(constants.LogGet, "Key not found for Get request key=%v, seqNum=%v", key, currentSeqNum)
				return ""
			}

			// Move to the next server (cyclically)
			server = (server + 1) % len(ck.servers)

			// Wait a short period to avoid overloading the system
			// This is especially important if the network is dropping packets
			// Don't use too long a timeout as we want to quickly find the leader
		}
	}
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
	// Increment sequence number for this request
	ck.seqNum++
	currentSeqNum := ck.seqNum

	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.clerkId,
		SeqNum:  currentSeqNum,
	}

	ck.logger.Log(constants.LogPutAppend, "Sending %v request for key=%v, value=%v, seqNum=%v", op, key, value, currentSeqNum)

	// Keep trying until we get a successful response
	for {
		// Try the last known leader first
		server := ck.lastKnownLeader

		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}

			ck.logger.Log(constants.LogPutAppend, "Trying server %d for %v request seqNum=%v", server, op, currentSeqNum)

			// Send the RPC to the server
			ok := ck.servers[server].Call(PutAppendRPC, &args, &reply)

			if ok && reply.Err == OK {
				// Successfully processed request
				ck.lastKnownLeader = server
				ck.logger.Log(constants.LogPutAppend, "%v request successful for key=%v, value=%v, seqNum=%v", op, key, value, currentSeqNum)
				return
			}

			// Move to the next server (cyclically)
			server = (server + 1) % len(ck.servers)

			// Wait a short period to avoid overloading the system
			// This is especially important if the network is dropping packets
			// Don't use too long a timeout as we want to quickly find the leader
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
