package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"

	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Role int

const (
	LEADER Role = iota
	CANDIDATE
	FOLLOWER
	UNKNOWN
)

type LogEntry struct {
	term uint32
	data string
}

type Node struct {
	candidateId string
	host        string
	port        string
	role        Role
}

type RaftPeer struct {
	// persistent state, for now keeping it in memory
	currentTerm uint32
	votedFor    string
	log         []LogEntry

	// volatile state, meant to be in-mem, can be lost
	commitIndex int32
	lastApplied int32

	role       Role
	roleUpdate chan Role // notify control loop when role changes

	// leader only volatile state
	nextIndex  []uint16
	matchIndex []uint16
	nodes      []Node

	// meta
	candidateId string
	timerFn     func() // election timer

	sync.RWMutex
}

// RPC Payloads

type RequestVoteRequest struct {
	Term         uint32 `json:"term"`
	CandidateId  string `json:"candidateId"`
	LastLogIndex uint32 `json:"lastLogIndex"`
	LastLogTerm  uint32 `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        uint32 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

type AppendEntriesRequest struct {
	Term         uint32     `json:"term"`
	LeaderId     string     `json:"leaderId"`
	PrevLogIndex uint32     `json:"prevLogIndex"`
	PrevLogTerm  uint32     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint32     `json:"leaderCommit"`
}

type AppendEntriesResponse struct {
	Term    uint32 `json:"term"`
	Success bool   `json:"success"`
}

// RPC Payloads End

func (server *RaftPeer) _setRole(role Role) {
	server.role = role
	server.roleUpdate <- role
}

func (server *RaftPeer) _resetElectionTimer() {
	server.timerFn() // cancel any running timers
	min := 1500
	max := 3000
	electionTimeout := rand.Intn(max-min+1) + min
	server.timerFn = CreateTimeoutCallback(electionTimeout, func() {
		server.Lock()
		defer server.Unlock()

		log.Print("Election timeout... Starting new election as a Candidate")
		server._setRole(CANDIDATE)
	})
}

func (server *RaftPeer) Start(port string) {
	http.HandleFunc("/requestVote", server.requestVoteHandler())
	http.HandleFunc("/appendEntries", server.appendEntriesHandler())
	http.HandleFunc("/ping", server.pingHandler())

	go http.ListenAndServe("0.0.0.0:"+port, nil)

	// artificial sleep so that we get enough time to start rest of the nodes
	time.Sleep(5 * time.Second)
	log.Print("Starting Raft Control...")
	server._resetElectionTimer()

	for {
		select {
		case role := <-server.roleUpdate:
			switch role {
			case LEADER:
				log.Print("Role change to LEADER")
				go server.leader()
			case CANDIDATE:
				log.Print("Role change to CANDIDATE")
				go server.candidate()
			case FOLLOWER:
				log.Print("Role change to FOLLOWER")
				server.follower()
			}
		default:
			// keep updating the commitIndex
			if server.commitIndex > server.lastApplied {
				server.lastApplied++
				// todo: apply log to the state machine
				log.Print("Apply log entry " + server.log[server.lastApplied].data)
			}
		}
	}
}

func (server *RaftPeer) leader() {
	// while leader, send hearbeat for now
	for server.role == LEADER {
		server.Lock()
		server._resetElectionTimer()
		server.Unlock()

		log.Print("Sending heartbeat to peers")
		server.appendEntries([]LogEntry{})
		time.Sleep(1000 * time.Millisecond)
	}
}

func (server *RaftPeer) candidate() {
	server.Lock()
	server.currentTerm++
	server._resetElectionTimer()
	server.Unlock()

	// send request vote to all peers
	server.requestVotes()
}

func (server *RaftPeer) follower() {
	// do nothing...for now
}

func (server *RaftPeer) requestVotes() {
	respChan := make(chan RequestVoteResponse, len(server.nodes))

	for _, node := range server.nodes {
		if node.candidateId == server.candidateId {
			continue
		}
		go func(node Node) {
			var lastLogIndex uint32 = 0
			var lastLogTerm uint32 = 0
			if len(server.log) > 0 {
				lastLogIndex = uint32(len(server.log) - 1)
				lastLogTerm = server.log[lastLogIndex].term
			}

			request := RequestVoteRequest{
				server.currentTerm,
				server.candidateId,
				lastLogIndex,
				lastLogTerm,
			}
			response, err := sendRequestVote(node, request)
			if err != nil {
				log.Error().Str("Peer", node.candidateId).Err(err).Msg("Error while sending RequestVote RPC to Peer")
				return
			}
			respChan <- response
		}(node)
	}

	// wait for responses
	votes := 1 // 1 for self vote
	for resp := range respChan {
		// if term is gt current term, update current term and become FOLLOWER
		if resp.Term > server.currentTerm {
			server.Lock()
			server.currentTerm = resp.Term
			server._setRole(FOLLOWER)
			log.Print("Received higher term from node, change to FOLLOWER")
			server.Unlock()
			return
		}

		// if vote granted, increment vote
		if resp.VoteGranted {
			votes++
			log.Printf("Votes: %d/%d", votes, len(server.nodes))
		}

		if votes > len(server.nodes)/2 {
			// majority, change to LEADER
			server.Lock()
			server._setRole(LEADER)
			log.Print("Received majority votes, change to LEADER")
			server.Unlock()
			return
		}
	}
}

func sendRequestVote(node Node, request RequestVoteRequest) (RequestVoteResponse, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s:%s/requestVote", node.host, node.port), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return RequestVoteResponse{}, err
	}
	defer resp.Body.Close()

	var response RequestVoteResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	return response, nil
}

func (server *RaftPeer) appendEntries(entries []LogEntry) {
	if server.role != LEADER {
		return
	}

	respChan := make(chan AppendEntriesResponse, len(server.nodes))
	// defer close(respChan)

	for _, node := range server.nodes {
		if node.candidateId == server.candidateId {
			continue
		}

		go func(node Node) {
			var prevLogIndex uint32 = 0
			var prevLogTerm uint32 = 0

			if len(server.log) > 0 {
				prevLogIndex = uint32(len(server.log) - 1)
				prevLogTerm = server.log[prevLogIndex].term
			}

			request := AppendEntriesRequest{
				server.currentTerm,
				server.candidateId,
				prevLogIndex,
				prevLogTerm,
				entries,
				uint32(server.commitIndex),
			}
			response, err := sendAppendEntries(node, request)
			if err != nil {
				log.Error().Err(err).Str("Peer", node.candidateId).Msg("Error while sending AppendEntries RPC to Peer")
				return
			}
			respChan <- response
		}(node)
	}

	// gather responses
	responded := 1
	for resp := range respChan {
		log.Print("Received response from node: ", responded, ":", resp.Success)
		responded++
	}
}

func sendAppendEntries(node Node, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s:%s/appendEntries", node.host, node.port), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer resp.Body.Close()

	var response AppendEntriesResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	return response, nil
}

func (server *RaftPeer) requestVoteHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request RequestVoteRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			log.Error().Err(err).Msg("Error while decoding RequestVote request")
			http.Error(w, "Error while decoding request", http.StatusBadRequest)
			return
		}

		response := server.handleRequestVote(request)
		jsonData, err := json.Marshal(response)
		if err != nil {
			log.Error().Err(err).Msg("Error while encoding RequestVote response")
			http.Error(w, "Error while encoding response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	})
}

func (server *RaftPeer) appendEntriesHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request AppendEntriesRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			log.Error().Err(err).Msg("Error while decoding AppendEntries request")
			http.Error(w, "Error while decoding request", http.StatusBadRequest)
			return
		}

		response := server.handleAppendEntries(request)
		jsonData, err := json.Marshal(response)
		if err != nil {
			log.Error().Err(err).Msg("Error while encoding AppendEntries response")
			http.Error(w, "Error while encoding response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	})
}

func (server *RaftPeer) pingHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
}

func (server *RaftPeer) handleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	log.Print("Received RequestVote from " + request.CandidateId)

	server.Lock()
	server._resetElectionTimer()
	server.Unlock()

	response := RequestVoteResponse{
		server.currentTerm,
		false,
	}

	// vote being asked for a higher term, become FOLLOWER
	if request.Term > server.currentTerm {
		server.Lock()
		server.currentTerm = request.Term
		server._setRole(FOLLOWER)
		server.Unlock()
	}

	// reject if request term is less than current tern
	if request.Term < server.currentTerm {
		return response
	}

	// haven't voted or already voted for the requesting candidate then return true
	if server.votedFor == "" || server.votedFor == request.CandidateId {
		server.Lock()
		server.votedFor = request.CandidateId
		server.Unlock()
		response.Term = server.currentTerm
		response.VoteGranted = true
	}

	return response
}

func (server *RaftPeer) handleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	server.Lock()
	defer server.Unlock()

	log.Print("Received AppendEntries request from " + request.LeaderId)
	server._resetElectionTimer()

	response := AppendEntriesResponse{
		server.currentTerm,
		false,
	}

	// if request term less, reject
	if request.Term < server.currentTerm {
		log.Print("Request term is less than current term")
		return response
	}

	if len(server.log) <= int(request.PrevLogIndex) || server.log[request.PrevLogIndex].term != request.PrevLogTerm {
		log.
			Info().
			Bool("len long is lt prevLI", len(server.log) <= int(request.PrevLogIndex)).
			Bool("prevLI term match req prevLogTerm", server.log[request.PrevLogIndex].term != request.PrevLogTerm).
			Msg("Log Mismatch")
		return response
	}

	// append entries to log
	// todo: handle conflicting changes
	server.log = append(server.log, request.Entries...)
	response.Success = true

	// update commitIndex
	// todo fix this
	if request.LeaderCommit > uint32(server.commitIndex) {
		server.commitIndex = int32(request.LeaderCommit) // min(leaderCommit, index of last new entry)
	}

	response.Term = server.currentTerm
	return response
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	if len(os.Args) < 2 {
		log.Error().Msg("Please provide the port number as a command-line argument.")
		os.Exit(1)
	}

	port := os.Args[1]

	// all nodes config
	config := []Node{
		{
			host: "localhost",
			port: "9000",
			role: UNKNOWN,
		},
		{
			host: "localhost",
			port: "9001",
			role: UNKNOWN,
		},
		{
			host: "localhost",
			port: "9002",
			role: UNKNOWN,
		},
	}

	// add candidate ids to the config
	for idx, _ := range config {
		node := config[idx]
		config[idx].candidateId = fmt.Sprintf("%s:%s", node.host, node.port)
	}

	server := RaftPeer{
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,

		role:       UNKNOWN,
		roleUpdate: make(chan Role, 1),

		nextIndex:  []uint16{},
		matchIndex: []uint16{},
		nodes:      config,

		candidateId: fmt.Sprintf("%s:%s", "localhost", port),
		timerFn:     func() {},
	}
	server.log[0].term = 0

	log.Print("Staring Raft server with candidate id " + server.candidateId)
	server.Start(port)
}
