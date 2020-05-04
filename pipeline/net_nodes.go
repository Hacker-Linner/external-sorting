package pipeline

import (
	"net"
	"bufio"
)

func NetworkSink(addr string, in <-chan int) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	// 开完 server 后，直接走人
	// goroutine 在后面等连接
	go func ()  {
		defer listener.Close()
		// 只是一次性的 pipeline
		// 来一个用户，就一口气把数据全部倒给你
		// 倒完就结束了，不接受新的连接了
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		writer := bufio.NewWriter(conn)
		defer writer.Flush()

		WriterSink(writer, in)
	}()
}

func NetworkSource(addr string) <-chan int {
	out := make(chan int)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		r := ReaderSource(
				bufio.NewReader(conn), -1)

		for v := range r {
			out <- v
		}
		close(out)
	}()
	return out
}