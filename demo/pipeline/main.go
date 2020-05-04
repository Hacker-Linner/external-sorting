package main

import (
	"external-sorting-on-k8s/pipeline"
	"fmt"
	"os"
)

func main() {
	file, err := os.Create("small.in")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.RandomSource(50)
	pipeline.WriterSink(file, p)
}

func mergeDemo() {
	// p := pipeline.ArraySource(3, 2, 4, 5, 1)
	// for {
	// 	if num, ok := <-p; ok {
	// 		fmt.Println(num)
	// 	} else {
	// 		// 管道结束了～
	// 		break
	// 	}
	// }

	// p := pipeline.InMemSort(
	// 	pipeline.ArraySource(3, 2, 6, 7, 4))

	p := pipeline.Merge(
		pipeline.InMemSort(
			pipeline.ArraySource(3, 2, 6, 7, 4)),
		pipeline.InMemSort(
			pipeline.ArraySource(7, 4, 0, 3, 2, 13, 8)))

	for v := range p {
		// 管道发送方一定明确 close
		fmt.Println(v)
	}
}
