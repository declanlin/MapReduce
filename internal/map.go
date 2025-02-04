package internal

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

func execMapTask(
	jobName string,
	mapTaskIdx int,
	rReduce int,
	inputFile string,
	mapFunc func(key string, value string) []KeyValue,
) {
	// Read the contents of the input file.

	fileContents, err := os.ReadFile(inputFile)
	if err != nil {
		log.Fatalf("failed to read input file %q: %v\n", inputFile, err)
	}

	// Apply the user-specified map function to the contents of the input file.

	kvSlice := mapFunc(inputFile, string(fileContents))

	// Emit each <K,V> pair to a temporary intermediate file on disk. I am choosing to use JSON to structure
	// my data.

	// To ensure constant access to an intermediate file associated with an arbitrary intermediate <K,V> pair, we can maintain
	// a map of the intermediate file names to their associated file pointers.

	intermediateFileMap := make(map[string]*os.File)

	for _, kvPair := range kvSlice {
		// Determine which reducer will handle the current <K,V> pair.
		// We can compute a reducer in the range [0, rReduce) to handle the <K,V> pair using
		// hash(K) % rReduce, where the hash function can be non-cryptographic since we are more concerned with
		// performance as opposed to security.

		reduceTaskIdx := fnv1a32Hash(kvPair.Key) % rReduce

		intermediateFile := generateIntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx)

		if _, ok := intermediateFileMap[intermediateFile]; !ok {
			fp, err := os.OpenFile(intermediateFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
			if err != nil {
				log.Fatalf("failed to open intermediate file %q for writing: %v\n", intermediateFile, err)
			}
			intermediateFileMap[intermediateFile] = fp
			defer fp.Close()
		}

		if intermediateFileMap[intermediateFile] != nil {
			// Encode the current <K,V> pair as a JSON object and emit it to the intermediate file.
			enc := json.NewEncoder(intermediateFileMap[intermediateFile])

			err = emitIntermediateJSON(&kvPair, enc)
			if err != nil {
				log.Fatalf("failed to emit <K,V> pair <%s,%s> as JSON to intermediate file %q: %v\n", kvPair.Key, kvPair.Value, intermediateFile, err)
			}
		}
	}

	// All pairs in the intermediate <K,V> space have been emitted to the appropriate intermediate files.
	// These files can now be read by the reducers to execute reduce tasks.
}

func emitIntermediateJSON(kvPair *KeyValue, enc *json.Encoder) error {

	if kvPair == nil {
		return nil
	}

	err := enc.Encode(kvPair)
	if err != nil {
		return err
	}
	return nil
}

func fnv1a32Hash(key string) int {
	hash := fnv.New32a()
	_, err := hash.Write([]byte(key))
	if err != nil {
		log.Fatalf("failed to generate 32-bit fnv-1a hash for key %q: %v\n", key, err)
	}

	return int(hash.Sum32() & 0x7fffffff)
}
