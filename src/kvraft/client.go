package kvraft

import (
	"crypto/rand"
	"math/big"

	"raft-6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeader int 
	// You will have to modify this struct.
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
	ck.lastLeader = -1
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
	for{

		args := &GetArgs{key}
		reply := &GetReply{} 

		if ck.lastLeader != -1 {
			ok := ck.servers[ck.lastLeader].Call("KVServer.Get",&args, &reply)
			if ok && reply.Err == "" {
				return reply.Value
			}
		}

		for i := range ck.servers {
			ok := ck.servers[i].Call("KVServer.Get",&args, &reply)
			if ok && reply.Err == "" {
				ck.lastLeader = i
				return reply.Value
			}
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
	// You will have to modify this function.
	for{

		args := &PutAppendArgs{key,value,op}
		reply := &PutAppendReply{} 

		if ck.lastLeader != -1 {
			ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend",&args, &reply)
			if ok && reply.Err == "" {
				return
			}
		}

		for i := range ck.servers {
			ok := ck.servers[i].Call("KVServer.PutAppend",&args, &reply)
			if ok && reply.Err == "" {
				ck.lastLeader = i
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
