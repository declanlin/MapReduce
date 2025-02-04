package internal

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

func (mr *Master) initRPCServer() {

	// Create a new RPC server and register the master node (and its associated methods) to it.

	rpcServer := rpc.NewServer()

	rpcServer.Register(mr)

	// Ensure that the socket file is not already bound.
	os.Remove(mr.nodeAddress)

	netListener, err := net.Listen("unix", mr.nodeAddress)
	if err != nil {
		log.Fatalf("failed to start the UNIX domain socket server at %s: %v\n", mr.nodeAddress, err)
	}

	mr.netListener = netListener

	// We are now listening for RPC calls on the master node's address.

	go func() {
		// Run the server in a loop until the shutdown signal is received through the shutdown channel.
	serverLoop:
		for {
			// Wait on a closure signal from the shutdown channel to break the server loop.
			// Pass by default to prevent blocking indefinitely within the select clause.
			// The select clause multiplexes multiple channels, so even if we are blocking on an Accept() call,
			// if the closure signal is received, we will break out of the master server loop and stop serving
			// incoming RPC calls.
			select {
			case <-mr.rpcShutdown:
				break serverLoop
			default:
			}

			// Each time an incoming RPC call is accepted, fork a thread that will handle the RPC request concurrently.
			conn, err := mr.netListener.Accept()
			if err == nil {
				go func() {
					// Serve the worker and then close the connection.
					rpcServer.ServeConn(conn)
					conn.Close()
				}()
			} else {
				fmt.Printf("failed to accept rpc connection on main server thread: %v\n", err)
				break
			}
		}

		// Stop accepting new connections if there was a server error when attempting to accept an incoming RPC connection.
		fmt.Printf("main server thread no longer accepting connections\n")
	}()
}

func (mr *Master) Shutdown() error {
	fmt.Print("shutting down master RPC server\n")

	close(mr.rpcShutdown)
	err := mr.netListener.Close()
	return err
}

func (mr *Master) shutdownRPCServer() {
	ok := sendRPCRequest(mr.nodeAddress, "Master.Shutdown", nil, nil)
	if !ok {
		fmt.Printf("Cleanup: RPC %s error\n", mr.nodeAddress)
	}
}
