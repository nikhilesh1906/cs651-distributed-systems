package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	kvs := make([]*KeyValue, 0, 2048)

	for m := 0; m < nMap; m++ {

		filename := reduceName(jobName, m, reduceTaskNumber)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(file)

		for {

			var kv KeyValue

			err := dec.Decode(&kv)

			if err != nil {
				if err == io.EOF {
					// 读取到末尾
					break
				}
				log.Fatal(err)
			}
			// 把新获取的值保存到 kvs 中
			kvs = append(kvs, &kv)

		}

		file.Close()
	}

	sort.Slice(kvs, func(i int, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	values := make([]string, len(kvs))
	for i := range values {
		values[i] = kvs[i].Value
	}
	var outFile string

	file, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)

	for i := range kvs {

		key := kvs[i].Key
		err := enc.Encode(KeyValue{key, reduceF(key, values)})

		if err != nil {
			log.Fatal(err)
		}
	}
}
