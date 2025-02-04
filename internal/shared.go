package internal

import "strconv"

type KeyValue struct {
	Key   string
	Value string
}

type taskPhase string

const (
	mapPhase    taskPhase = "mapPhase"
	reducePhase taskPhase = "reducePhase"
)

func generateIntermediateFileName(jobName string, mapTaskIdx int, reduceTaskIdx int) string {
	return "mr-intm-" + jobName + "-f" + strconv.Itoa(mapTaskIdx) + "-r" + strconv.Itoa(reduceTaskIdx)
}

func generateReduceTaskOutputFileName(jobName string, reduceTaskIdx int) string {
	return "mr-rout-" + jobName + "-r" + strconv.Itoa(reduceTaskIdx)
}
