package main

import (
	"bufio"
	"external-sorting-on-k8s/pipeline"
	"fmt"
	"os"
	"strconv"
)

func main() {
	// p := createPipeline(
	// 	"small.in", 512, 4)
	// writeToFile(p, "small.out")
	// printFile("small.out")

	// p := createPipeline(
	// 	"large.in", 800000000, 4)
	// writeToFile(p, "large.out")
	// printFile("large.out")

	// test
	// createNetworkPipeline("small.in", 512, 4)
	// time.Sleep(time.Hour)

	// p := createNetworkPipeline("small.in", 512, 4)
	// writeToFile(p, "small.out")
	// printFile("small.out")

	p := createNetworkPipeline("large.in", 800000000, 4)
	writeToFile(p, "large.out")
	printFile("large.out")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriterSink(writer, p)
}

func createPipeline(
	filename string,
	filesize, chunkCount int) <-chan int {
	chunkSize := filesize / chunkCount
	pipeline.Init()

	sortResults := []<-chan int{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		// i*chunkSize-> 表明从第几块开始
		// 0 -> 表示从头开始
		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		// 收集结果
		sortResults = append(sortResults, pipeline.InMemSort(source))
	}
	return pipeline.MergeN(sortResults...)
}

func createNetworkPipeline(
	filename string,
	filesize, chunkCount int) <-chan int {
	chunkSize := filesize / chunkCount
	pipeline.Init()

	sortAddr := []string{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000+i)
		// 开 server
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}

	sortResults := []<-chan int{}
	for _, addr := range sortAddr {
		// 开 client 连接 server
		sortResults = append(
			sortResults,
			pipeline.NetworkSource(addr),
		)
	}

	return pipeline.MergeN(sortResults...)
}
