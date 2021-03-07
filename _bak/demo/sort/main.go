package main

import (
	"fmt"
	"sort"
)

func main() {
	nums := []int{10, 2, 3, 4, 5, 1, 6}
	sort.Ints(nums)

	for _, v := range nums {
		fmt.Println(v)
	}
}