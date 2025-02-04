package internal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

// Create the structure for the Master node.

type Master struct {

	// Address that uniquely identifies a master node.
	nodeAddress string

	// Channel for goroutines to communicate job completion status.
	doneChannel chan bool

	// Name of the mapreduce job being completed.
	jobName string

	// The set of input files that gets fed in to the mappers.
	inputFiles []string

	// The number of partitions to distribute the intermediate key space into.
	rReduce int

	// For the distributed version, we want to register workers with the master node
	// so that the master node can manage their states and assign tasks. Multiple scheduled
	// worker threads may try to access the master's worker list or wait for an available worker,
	// so we need a mutex to protect these resources and avoid race conditions.

	sync.Mutex

	// Slice of each worker's RPC address.
	workers []string

	// A condition variable is a synchronization mechanism in Go that allows one or more threads to
	// wait for a certain condition to be met, and be notified that the condition has been met. In the context
	// of the mapreduce application, we want to notify scheduling threads when a worker is available so that it
	// can be registered with the master RPC server.

	newWorkerCond *sync.Cond

	// Channel to listen for master RPC server shutdown signal.

	rpcShutdown chan struct{}

	// Network listener to accept RPC calls to the master node.

	netListener net.Listener

	stats []int

	// Time that the master registers the first worker and begins assigning tasks.
	StartTime time.Time
}

func initMaster(nodeAddress string) (mr *Master) {
	mr = new(Master)
	mr.nodeAddress = nodeAddress
	mr.doneChannel = make(chan bool)

	mr.newWorkerCond = sync.NewCond(mr)
	mr.rpcShutdown = make(chan struct{})

	return
}

func Sequential(
	nodeAddress string,
	jobName string,
	inputFiles []string,
	rReduce int,
	mapFunc func(key string, value string) []KeyValue,
	reduceFunc func(key string, values []string) string,
) (mr *Master) {
	mr = initMaster(nodeAddress)

	// Run a goroutine that executes the mapreduce job sequentially.
	// Map tasks are scheduled and completed sequentially, followed by reduce tasks
	// that are scheduled and completed sequentially.

	go mr.exec(jobName, inputFiles, rReduce, func(taskPhase taskPhase) {

		// Schedule tasks based on their phase.
		switch taskPhase {
		case mapPhase:
			// Each input file becomes associated with a map task. We want to execute all maps tasks.
			for mapTaskIdx, inputFile := range mr.inputFiles {
				// To execute a map task, we want to apply the user-specified mapper function to the contents of each
				// task's associated input file, which will produce intermediate <K,V> pairs that are stored on disk
				// and later read by the reducers.

				// ex. mr-intm-wordcount-f0-r1

				// The above example is a file containing all emitted intermediate <K,V> pairs
				// from the input file 0 (map task 0) that have been assigned to reducer 1.

				execMapTask(jobName, mapTaskIdx, mr.rReduce, inputFile, mapFunc)
			}
		case reducePhase:
			// Each intermediate file becomes associated with a reduce task. We want to execute all reduce tasks.
			for reduceTaskIdx := 0; reduceTaskIdx < mr.rReduce; reduceTaskIdx++ {

				// To execute a reduce task, we want to apply the user-specified reducer function to each partition of the
				// intermediate key space generated across the mMap map tasks.

				execReduceTask(jobName, reduceTaskIdx, len(mr.inputFiles), reduceFunc)
			}
		}
	}, func() {
		mr.stats = []int{len(inputFiles) + rReduce}
	})

	return
}

func (mr *Master) exec(
	jobName string,
	inputFiles []string,
	rReduce int,
	scheduleFunc func(taskPhase taskPhase),
	cleanup func(),
) {
	// To complete a mapreduce job, whether we are running in sequential or distributed mode, we
	// need to schedule and complete all map tasks, followed by scheduling and completing all reduce tasks.

	// Initialize job related data.
	mr.jobName = jobName
	mr.inputFiles = inputFiles
	mr.rReduce = rReduce

	fmt.Printf("%s: Starting mapreduce job %q\n", mr.nodeAddress, mr.jobName)

	// Sequentially schedule and execute all map tasks.
	scheduleFunc(mapPhase)

	// Sequentially schedule and execute all reduce tasks.
	scheduleFunc(reducePhase)

	// TODO: CLEANUP FUNCTION

	cleanup()

	// TODO: MERGE REDUCER OUTPUTS INTO FINAL OUTPUT FILE

	mr.merge()

	// Indicate that the job is completed by sending a message to the blocking wait function via the done channel.
	mr.doneChannel <- true

	fmt.Printf("%s: Completed mapreduce job %q\n", mr.nodeAddress, mr.jobName)
}

// Blocking function that waits for all scheduled tasks and cleanup functions to complete.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// When a worker is up and ready to receive tasks, it will register itself with the master through an RPC call.

func (mr *Master) Register(args *WorkerRegisterArgs, _ *struct{}) error {
	// Lock the mutex-embedded master node. This is because many worker threads may try to modify the master's workers list
	// at the same time, and we must lock the access to one thread to avoid race conditions.
	mr.Lock()
	defer mr.Unlock()

	fmt.Printf("Registering worker: %s\n", args.NodeAddress)
	mr.workers = append(mr.workers, args.NodeAddress)

	// The function forwardRegistrations waits on the condition that a worker registers with the master through an RPC call.
	// To signal to this function that the condition has been satisfied, we need to broadcast to the waiting goroutine.
	mr.StartTime = time.Now()

	mr.newWorkerCond.Broadcast()

	return nil
}

// This function communicates with the scheduler through a shared channel that gets passed to the scheduler, which
// the scheduler reads from to determine information about the worker node.

func (mr *Master) forwardWorkerRegistrationsToScheduler(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			workerAddress := mr.workers[i]
			// Send the worker address to the scheduler via the channel.
			go func() { ch <- workerAddress }()
			i = i + 1
		} else {
			// Wait until a worker registers with the master node via an RPC call and the master broadcasts
			// a signal that a worker has registered.
			mr.newWorkerCond.Wait()
		}
		mr.Unlock()
	}
}

func Distributed(
	jobName string,
	inputFiles []string,
	rReduce int,
	masterNodeAddress string,
) (mr *Master) {
	mr = initMaster(masterNodeAddress)
	mr.initRPCServer()
	go mr.exec(jobName, inputFiles, rReduce,
		func(taskPhase taskPhase) {
			ch := make(chan string)
			go mr.forwardWorkerRegistrationsToScheduler(ch)
			taskScheduler(mr.jobName, mr.inputFiles, mr.rReduce, taskPhase, ch)
		},
		func() {
			mr.stats = mr.shutdownWorkers()
			mr.shutdownRPCServer()
		},
	)
	return
}

// // If we shut down the master RPC server, we need to shut down each of the workers by sending them their respective RPC
// // requests to invoke their shutdown handlers.

func (mr *Master) shutdownWorkers() []int {

	// Lock the mutex-embedded master node.
	mr.Lock()
	defer mr.Unlock()

	nTasks := make([]int, 0, len(mr.workers))
	for _, worker := range mr.workers {
		fmt.Printf("Master: shutting down worker %s\n", worker)
		var shutdownReply WorkerShutdownReply
		ok := sendRPCRequest(worker, "Worker.Shutdown", new(struct{}), &shutdownReply)
		if !ok {
			fmt.Printf("Master: worker %s RPC shutdown error\n", worker)
		} else {
			nTasks = append(nTasks, shutdownReply.NTasks)
		}
	}
	return nTasks
}

// merge combines the results of the many reduce jobs into a single output file
// XXX use merge sort
func (mr *Master) merge() {
	kvs := make(map[string]string)
	for i := 0; i < mr.rReduce; i++ {
		p := generateReduceTaskOutputFileName(mr.jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create("mrtmp." + mr.jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}
