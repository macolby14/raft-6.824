package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"raft-6.824/labgob"
	"raft-6.824/labrpc"
	"raft-6.824/raft"
)

/*

 */


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	OpKey string
	OpVal string
}

func (op *Op) String() string{
	return fmt.Sprintf("%v %v %v",op.OpType,op.OpKey,op.OpVal)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store 	map[string]string
	status 	map[string]bool
}


/*
Get and PutAppend
Call raft.Start() -> func (rf *Raft) Start(command interface{}) (int, int, bool)
- First arg is index if it is ever commited
- Second arg is term
- Third arg is if it thinks it is the leader

If raft is not the leader, return (Error, nil). Client will handle retrying
	- We could point at who this server's raft voted for error, but not authoritative


If raft thinks it is the leader and is the leader, we will receive ApplyMsg{CommandValid bool, Command interface{}, index} 

We can block by waiting for an applyCh value.

Concurrency?
- We submit a command
- Another server submits a command
- Raft logs the other servers command and we see it on the applyCh
- We see our command on the applyCh

Solution: 
- Check the commitIndex to verify this is the correct command. If not, keep waiting.
- We need to handle any other comamnds that change the KV state. Since they overlap, it is fine to return the other value first.
- Commit our value to the state and then return.
*/


func (kv *KVServer) init() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			panic("Invalid command in applyCh not expected")
		}
		cmd := msg.Command.(Op)
		kv.mu.Lock()
		switch cmd.OpType{
		case "get": 
		case "put": kv.store[cmd.OpKey] = cmd.OpVal
		case "append": kv.store[cmd.OpKey] = kv.store[cmd.OpKey] + cmd.OpVal
		default: panic("Invalid command opType")
		}
		kv.status[fmt.Sprint(msg.CommandIndex)+fmt.Sprint(cmd)] = true
		kv.mu.Unlock()
	}
}

/**
args - (Key)
reply - (Err, Value)
*/
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := &Op{"get", args.Key,""}
	commitInd, _, isLeader := kv.rf.Start(*op)
	if !isLeader{
		reply.Err = "Not leader"
		return
	}
	// I think commitInd and term together are a good unique kep
	statusKey := fmt.Sprint(commitInd)+fmt.Sprint(op)
	kv.mu.Lock()
	kv.status[statusKey] = false
	kv.mu.Unlock()

	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		if kv.status[statusKey] {
			v, exists := kv.store[args.Key]
			if !exists {
				reply.Value = ""
			}else{
				reply.Value = v
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
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

	// You may need initialization code here.

	return kv
}
