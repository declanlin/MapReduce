// package internal

// import "sync"

// type WorkerLoad struct {
// 	mu  sync.Mutex
// 	now int32
// 	max int32
// }

package internal

//
// Please do not modify this file.
//

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// track whether workers executed in parallel.
type Parallelism struct {
	mu  sync.Mutex
	now int32
	max int32
}

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	sync.Mutex

	name        string
	Map         func(string, string) []KeyValue
	Reduce      func(string, []string) string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	concurrent  int // number of parallel DoTasks in this worker; mutex
	l           net.Listener
	parallelism *Parallelism
}

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(arg *ExecTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.TaskPhase, arg.TaskIdx, arg.InputFile, arg.OtherPhaseNTask)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now += 1
		if wk.parallelism.now > wk.parallelism.max {
			wk.parallelism.max = wk.parallelism.now
		}
		if wk.parallelism.max < 2 {
			pause = true
		}
		wk.parallelism.mu.Unlock()
	}

	if pause {
		// give other workers a chance to prove that
		// they are executing in parallel.
		time.Sleep(time.Second)
	}

	switch arg.TaskPhase {
	case mapPhase:
		execMapTask(arg.JobName, arg.TaskIdx, arg.OtherPhaseNTask, arg.InputFile, wk.Map)
	case reducePhase:
		execReduceTask(arg.JobName, arg.TaskIdx, arg.OtherPhaseNTask, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now -= 1
		wk.parallelism.mu.Unlock()
	}

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.TaskPhase, arg.TaskIdx)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *WorkerShutdownReply) error {
	fmt.Printf("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.NTasks = wk.nTasks
	wk.nRPC = 1
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(WorkerRegisterArgs)
	args.NodeAddress = wk.name
	ok := sendRPCRequest(master, "Master.Register", args, new(struct{}))
	if !ok {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int, parallelism *Parallelism,
) {
	fmt.Printf("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	wk.parallelism = parallelism
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
	fmt.Printf("RunWorker %s exit\n", me)
}
