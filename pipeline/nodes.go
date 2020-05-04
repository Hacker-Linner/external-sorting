package pipeline

import "sort"

func ArraySource(a ...int) <-chan int {
	// <-chan: 表明用它的人，只能拿东西
	out := make(chan int)
	go func ()  {
		for _, v := range a {
			// 这里我们就只能放东西
			out <- v
		}
		close(out) // 又明确的结尾，所以这里关掉
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int)

	go func ()  {
		// Read into memory
		a := []int{}
		for v := range in {
			a = append(a, v)
		}

		// Sort
		sort.Ints(a)
		
		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()

	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int)

	go func ()  {
		v1, ok1 := <- in1
		v2, ok2 := <- in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <- in1
			} else {
				out <- v2
				v2, ok2 = <- in2
			}
		}
		close(out)
	}()
	return out
}