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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft-6.824/labrpc"
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
	Command interface{}
}

func (l Log) String() string {
	cmdStr := fmt.Sprintf("%v", l.Command)
	cmdStr = cmdStr[0:int(math.Min(float64(len(cmdStr)), 10))]
	return fmt.Sprintf("{Term: %v, Command: %v}", l.Term, cmdStr)
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

	leader        LeaderState //candidate, follower, or leader
	lastHeartbeat time.Time   // last time received a message from the leader, chosen to start an election

	applyCh chan ApplyMsg
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
		if rf.state == "leader" {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
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
	// rf.mu.Lock()
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// rf.mu.Unlock()
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	// 	return
	// }
	// // Your code here (2C).
	// // Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var log []Log
	// if d.Decode(&currentTerm) != nil ||
	// 	d.Decode(&votedFor) != nil ||
	// 	d.Decode(&log) != nil {
	// 	panic("Error decoding in readPersist")
	// } else {
	// 	rf.mu.Lock()
	// 	rf.currentTerm = currentTerm
	// 	rf.votedFor = votedFor
	// 	rf.log = log
	// 	rf.mu.Unlock()
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
	if rf.currentTerm > args.Term { // an election from a previous term, need to update the node making the call
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm { // we haven't voted for this term yet, so lets vote
		lastLogTerm := -1
		lastLogIndex := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
			lastLogIndex = len(rf.log)
		}

		rf.currentTerm = args.Term
		rf.state = "follower"
		reply.Term = args.Term

		if args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
			DPrintf("%v (%v): Denying vote to %v because its log is not up to date. Other (Term and Index) (%v %v). My (Term and Index): (%v %v)\n", rf.me, rf.state, args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		}
	} else { //already voted for this term
		// TODO - Why do we think we already voted? We could have restarted and need to check our voted state
		DPrintf("%v (%v): Denying vote to %v because we already voted for %v.\n", rf.me, rf.state, args.CandidateId, rf.votedFor)
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
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := &RequestVoteArgs{}
			args.LastLogIndex = len(rf.log)
			if len(rf.log) > 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			} else {
				args.LastLogTerm = -1
			}
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			reply := &RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok { // no response from rpc
				return
			}
			if args.Term != rf.currentTerm { // candidate changed terms during an election
				rf.state = "follower"
				return
			}
			if rf.state == "leader" { // we already won this election
				return
			}
			DPrintf("%v: Received reply to vote request. %v Vote Granted: %v. Reply Term: %v", rf.me, i, reply.VoteGranted, reply.Term)
			if reply.VoteGranted {
				voteCt++
			} else if reply.Term > rf.currentTerm { // we are behind in terms
				rf.currentTerm = reply.Term
				rf.state = "follower"
			}
			if rf.currentTerm == args.Term && voteCt >= majority {
				DPrintf("%v: Won the election", rf.me)
				rf.state = "leader"
				rf.leader.init(rf)
				go rf.sendHeartbeats()
			}
		}(i)
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
	XTerm   int  // term in the concluding entry
	XIndex  int  // index of first entry with term
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	DPrintf("%v (%v): Received heartbeat from %v. Entries: %v. PrevLogIndex: %v. PrevLogTerm: %v", rf.me, rf.state, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	if args.Term < rf.currentTerm { // heartbeat from an old term, update the calling node
		return
	}

	/*
		- Case 1: Has the log and is the correct term. Is up to date
		- Case 2: Has the log, but different term. Need to back up.
		- Case 3: Does not have the log.
	*/

	rf.lastHeartbeat = time.Now()
	rf.state = "follower" // if we get a heartbeat from someone else ahead of us in terms... they were elected leader

	// Follower is missing some logs. Follower doesn't have what the leader wants to check
	if args.PrevLogIndex > len(rf.log) {
		return
	}

	// Follower has a different log at PrevLogIndex than leader
	if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		rf.log = rf.log[0 : args.PrevLogIndex-1]
		return
	}

	reply.Success = true
	rf.log = append(rf.log, args.Entries...)
	if len(rf.log) <= args.LeaderCommit && len(rf.log) > 0 {
		oldCommit := rf.commitIndex
		rf.commitIndex = len(rf.log)
		for i, log := range rf.log[oldCommit:rf.commitIndex] {
			applied := &ApplyMsg{true, log.Command, oldCommit + 1 + i}
			rf.applyCh <- *applied
		}

	}
	DPrintf("%v (%v): Append/Heartbeat succeeded. My log: %v. My commitIndex: %v", rf.me, rf.state, rf.log, rf.commitIndex)
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
			rf.mu.Unlock()

			go func(i int) {
				rf.mu.Lock()
				if rf.killed() {
					return
				}
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.leader.nextIndex[i] - 1
				DPrintf("%v (%v): Sending Heartbeats. My term: %v. My log: %v. My commitIndex: %v", rf.me, rf.state, rf.currentTerm, rf.log, rf.commitIndex)
				if args.PrevLogIndex == 0 {
					args.PrevLogTerm = -1
				} else {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				if len(rf.log) > rf.leader.nextIndex[i]-1 {
					args.Entries = rf.log[rf.leader.nextIndex[i]-1 : len(rf.log)]
				} else {
					args.Entries = make([]Log, 0)
				}
				args.LeaderCommit = rf.commitIndex
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					defer rf.mu.Unlock()
					rf.state = "follower"
					rf.currentTerm = reply.Term
					return
				}
				if reply.Success {
					DPrintf("%v (%v): Heartbeat to %v succeeds.", rf.me, rf.state, i)

					rf.leader.nextIndex[i] += len(args.Entries)
					rf.leader.matchIndex[i] = rf.leader.nextIndex[i] - 1

					for rf.commitIndex+1 <= rf.leader.matchIndex[i] {
						agreeCt := 0
						for j := range rf.leader.matchIndex {
							if j == rf.me || rf.leader.matchIndex[j] >= rf.commitIndex+1 {
								agreeCt++
							}
						}
						DPrintf("%v: Agree Ct to see if commit: %v. Commit Index: %v", rf.me, agreeCt, rf.commitIndex)
						if rf.leader.matchIndex[i] != 0 && agreeCt > len(rf.peers)/2 {
							applied := &ApplyMsg{true, rf.log[rf.commitIndex].Command, rf.commitIndex + 1}
							rf.commitIndex++
							rf.applyCh <- *applied
						} else {
							break
						}
					}
				} else {
					DPrintf("%v: Append to %v fails.", rf.me, i)
					rf.leader.nextIndex[i]--
				}
				rf.mu.Unlock()
			}(i)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		return -1, -1, false
	}

	newLog := &Log{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, *newLog)
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true

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
	DPrintf("%v (%v): Killed. Log: %v Commited: %v\n", rf.me, rf.state, rf.log, rf.commitIndex)
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
