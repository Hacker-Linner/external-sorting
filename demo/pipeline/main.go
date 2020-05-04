package main

import (
	"bufio"
	"external-sorting-on-k8s/pipeline"
	"fmt"
	"os"
)

func main() {
	const filename = "large.in"
	// 弄个 100M 的数据
	const n = 100000000

	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.RandomSource(n)
	// bufio 设置一个默认的缓冲区
	writer := bufio.NewWriter(file)
	pipeline.WriterSink(writer, p)
	// 注意清空缓冲区，确保所有数据都倒出去
	writer.Flush()

	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p = pipeline.ReaderSource(bufio.NewReader(file))
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
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
