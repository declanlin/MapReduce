package internal

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func execReduceTask(
	jobName string,
	reduceTaskIdx int,
	mMap int,
	reduceFunc func(key string, values []string) string,
) {
	// We want to read from disk the contents of the intermediate files associated with the current reduce task.

	// For each unique key in the intermediate key space, map it to a slice containing all values associated with that key.

	uniqueKeyValuesMap := make(map[string][]string)

	for mapTaskIdx := 0; mapTaskIdx < mMap; mapTaskIdx++ {
		// Read an intermediate file associated with the current reduce task from disk.

		intermediateFile := generateIntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx)
		fp, err := os.Open(intermediateFile)
		if err != nil {
			log.Fatalf("failed to open intermediate file %q for reading in reducer: %v\n", intermediateFile, err)
		}
		defer fp.Close()

		// For each emitted <K,V> pair in the intermediate file, decode it into the map.
		absorbIntermediateJSON(fp, &uniqueKeyValuesMap)
	}

	// Since we also want the output of each reduce task to be sorted, we can maintain an auxiliary array of the unique keys themselves,
	// which can be sorted and used to access the values associated with each of these unique keys.
	var uniqueKeys []string

	for key := range uniqueKeyValuesMap {
		uniqueKeys = append(uniqueKeys, key)
	}

	// Sort the unique keys by increasing lexicographical order.

	sort.Strings(uniqueKeys)

	// Apply the user-specified reduce function to each <uK,[V]> pair to generate the final value, fV, associated with that
	// unique key, uK. Emit this <uK,fV> pair to the output file associated with the current reduce task.

	reduceTaskOutputFileName := generateReduceTaskOutputFileName(jobName, reduceTaskIdx)
	fp, err := os.OpenFile(reduceTaskOutputFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)

	if err != nil {
		log.Fatalf("failed to open output file %q for reducer task: %v\n", reduceTaskOutputFileName, err)
	}
	defer fp.Close()

	if fp == nil {
		return
	}

	enc := json.NewEncoder(fp)

	for _, uniqueKey := range uniqueKeys {

		// Generate the <uK,V> pair containing the reduced V value for the unique key uK.

		kvPairFinal := KeyValue{uniqueKey, reduceFunc(uniqueKey, uniqueKeyValuesMap[uniqueKey])}

		// Emit the reduced <K,V> pair to the output file associated with the current reduce task.

		err := emitIntermediateJSON(&kvPairFinal, enc)
		if err != nil {
			log.Fatalf("failed to emit json to reduce task output file %q: %v", reduceTaskOutputFileName, err)
		}
	}
}

// Absorbs all intermediate JSON <K,V> pairs from the intermediate file into the unique key values map.

func absorbIntermediateJSON(fp *os.File, uniqueKeyValuesMap *map[string][]string) {
	if fp == nil {
		return
	}

	dec := json.NewDecoder(fp)
	// Decode JSON <K,V> pairs from the intermediate file while there are more pairs to be parsed.
	for dec.More() {
		var kvPair KeyValue
		dec.Decode(&kvPair)
		(*uniqueKeyValuesMap)[kvPair.Key] = append((*uniqueKeyValuesMap)[kvPair.Key], kvPair.Value)
	}
}
