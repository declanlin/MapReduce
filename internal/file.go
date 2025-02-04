package internal

import (
	"log"
	"os"
)

func removeFile(file string) {
	err := os.Remove(file)

	if err != nil {
		log.Fatalf("failed to remove file %q from system: %v\n", file, err)
	}
}

func (mr *Master) DeleteFiles() {
	for i := range mr.inputFiles {
		for j := 0; j < mr.rReduce; j++ {
			removeFile(generateIntermediateFileName(mr.jobName, i, j))
		}
	}

	for i := 0; i < mr.rReduce; i++ {
		removeFile(generateReduceTaskOutputFileName(mr.jobName, i))
	}

	// removeFile("mrtmp." + mr.jobName)
}
