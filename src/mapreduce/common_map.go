package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks must be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	data, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}

	// 利用 mapF 函数处理 inFile 文件内容，得到 kvs
	kvs := mapF(inFile, string(data))

	// 把 kvs 按照 R 任务的需求，划分成 nReduce 份
	for i := 0; i < nReduce; i++ {
		// Reduce 任务 i ，创建文件
		filename := reduceName(jobName, mapTaskNumber, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}

		// 把 file 作为 json 流式编码器的输出目的地
		enc := json.NewEncoder(file)

		for _, kv := range kvs {
			if ihash(kv.Key)%nReduce != i {
				// 根据题意，
				// 删除掉不需要 Reduce 任务 i 处理的 kv
				continue
			}

			// 对 kv 进行编码后，发送到 file
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}

		// Map 任务 m 为 Reduce 任务 i 准备处理的内容
		// 已经全部收集完毕，所以，可以关闭 file 了
		file.Close()

	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
