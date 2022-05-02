package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Term    int
	Command string
}

type LeaderState struct { // volatile, reinitiailized after elections
	rf         *Raft
	nextIndex  []int // for each server, index of next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicate on server
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//other
	state string

	//persisted
	currentTerm int   // latest term server has seen initalized
	votedFor    int   // candidateId that received vote in current term (or null if none)
	log         []Log // log entries; each contains command and term when entry received by leader

	// volatile
	commitIndex int //highest log entry known to be commited
	lastApplied int //inderx of highest log entry applied to state machine

	leader          LeaderState //candidate, follower, or leader
	electionTimeout int         // random time in ms before starts an election
	lastHeartbeat   time.Time   // last time received a message from the leader, chosen to start an election
}

func (rf *Raft) init() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = 0
	rf.log = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = "follower"
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	go rf.checkTimeout()
}

func (rf *Raft) checkTimeout() bool {
	for {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		electionTimeout := rand.Intn(2000) + 500 // timeout in ms
		rf.mu.Unlock()
		if time.Now().Sub(lastHeartbeat).Milliseconds() > int64(electionTimeout) {
			DPrintf("%v: Time passed: %v. electionTimeout: %v", rf.me, time.Now().Sub(lastHeartbeat).Milliseconds(), electionTimeout)
			rf.initiateElection()
		}
	}
}

func (ls *LeaderState) init(rf *Raft) {
	ls.rf = rf
	ls.nextIndex = make([]int, len(ls.rf.peers))
	for i := range ls.nextIndex {
		ls.nextIndex[i] = len(ls.rf.log) + 1
	}
	ls.matchIndex = make([]int, len(ls.rf.peers))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = rf.currentTerm
	var isleader = rf.state == "leader"
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidates term
	CandidateId  int //candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means canddiate receiced vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term { // an election from a previous term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm { // we haven't voted for this term yet
		rf.state = "follower"
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = args.Term
	} else { //already voted for this term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) initiateElection() {
	rf.mu.Lock()
	DPrintf("%v: Initating election", rf.me)
	rf.lastHeartbeat = time.Now()
	majority := int(math.Ceil(float64(len(rf.peers)) / 2.0))
	rf.currentTerm = rf.currentTerm + 1
	rf.state = "candidate"
	rf.votedFor = rf.me
	voteCt := 1
	rf.mu.Unlock()
	// TODO - RPC calls in goroutine so they don't block
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{}
		rf.mu.Lock()
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		rf.mu.Unlock()
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(i, args, reply)
		if !ok { // no response
			continue
		}
		rf.mu.Lock()
		DPrintf("%v: Received reply to vote request. %v %v", rf.me, i, reply)
		if reply.VoteGranted {
			voteCt++
		} else if reply.Term > rf.currentTerm { // we are behind in terms
			rf.currentTerm = reply.Term
			rf.state = "follower"
			rf.mu.Unlock()
			return
		}
		if voteCt >= majority {
			DPrintf("%v: Won the election", rf.me)
			rf.state = "leader"
			rf.mu.Unlock()
			go rf.sendHeartbeats()
			return
		}
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately precenting new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // empty for heartbeat, may send more than 1 for efficiency
	LeaderCommit int   //leader's commit index
}

type AppendEntriesReply struct {
	Term    int  //current term, for leader to update itself
	Success bool // true if folloewr contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v: Received heartbeat. %v", rf.me, args)
	if args.Term < rf.currentTerm {
		defer rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.state != "leader" {
			defer rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			rf.mu.Lock()
			if i == rf.me {
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
				continue
			}
			args := &AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(i, args, reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				defer rf.mu.Unlock()
				rf.state = "follower"
				rf.currentTerm = reply.Term
				return
			}
			rf.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
