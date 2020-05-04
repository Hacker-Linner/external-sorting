package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

var startTime time.Time

func Init() {
	startTime = time.Now()
}

func ArraySource(a ...int) <-chan int {
	// <-chan: 表明用它的人，只能拿东西
	out := make(chan int)
	go func() {
		for _, v := range a {
			// 这里我们就只能放东西
			out <- v
		}
		close(out) // 又明确的结尾，所以这里关掉
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 1024)

	go func() {
		// Read into memory
		a := []int{}
		for v := range in {
			a = append(a, v)
		}
		fmt.Println("Read done:", time.Now().Sub(startTime))

		// Sort
		sort.Ints(a)
		fmt.Println("InMemSort done:", time.Now().Sub(startTime))

		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()

	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)

	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		close(out)
		fmt.Println("Merge done:", time.Now().Sub(startTime))
	}()
	return out
}

func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024) // 加个 buffer, 不要发一个收一个
	go func() {
		// int 是 32 位 还是 64 位，这个根据系统来
		// 当前是 64 位的，所以这里开一个 64 位的 buffer
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			// n: 读了几个字节
			// err: 是否有错误
			n, err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				// 这里选用大端，读写统一就行
				// 然后转成一个有符号的 int
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			// 如果最后只有4个字节，就 EOF 了
			// 这里假设 -1 表示全部读
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

func WriterSink(writer io.Writer, in <-chan int) {
	for v := range in {

		buffer := make([]byte, 8)

		binary.BigEndian.PutUint64(buffer, uint64(v))

		writer.Write(buffer)
	}
}

func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

func MergeN(inputs ...<-chan int) <-chan int {

	if len(inputs) == 1 {
		return inputs[0]
	}

	m := len(inputs) / 2
	// merge inputs[0...m) and inputs [m...end)
	return Merge(
		MergeN(inputs[:m]...),
		MergeN(inputs[m:]...))
}
