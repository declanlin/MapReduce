package internal

import (
	"fmt"
	"net/rpc"
)

type ExecTaskArgs struct {
	JobName   string
	InputFile string
	TaskPhase taskPhase
	TaskIdx   int

	// If we are in the map phase, we want to know how many bins to distribute the intermediate
	// key space into (rReduce).

	// If we are in the reduce phase, we want to know how many input files to read from disk for each
	// reducer (mMap).

	OtherPhaseNTask int
}

// When shutting a worker down, we want to obtain information about the worker's activities during its lifetime.

type WorkerShutdownReply struct {
	// Total number of tasks completed by the worker.
	NTasks int
}

// The argument set that gets passed to the master when registering a worker with the master.
type WorkerRegisterArgs struct {
	// The RPC address of the worker (its UNIX-domain socket name).
	NodeAddress string
}

// Wrapper around RPC functions to send an RPC to the specified handler on the master server thread.
// The function returns true if the server responded, and false if the server did not respond.

func sendRPCRequest(masterNodeAddress string, rpcHandler string, args interface{}, reply interface{}) bool {
	// Attempt to initiate a connection with the master RPC server.
	conn, err := rpc.Dial("unix", masterNodeAddress)
	if err != nil {
		return false
	}
	defer conn.Close()

	// Send the request to the server to invoke the specified handler with any arguments. Store the server reply.

	err = conn.Call(rpcHandler, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
