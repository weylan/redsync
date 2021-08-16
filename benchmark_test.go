package redsync

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkMymutex(b *testing.B) {
	b.StopTimer()

	//cases := makeCases(2)
	//		"redigo"
	//		"goredis"
	//		"goredis_v7"
	//		"goredis_v8"
	//v := cases["goredis_v7"]
	//rs := New(v.pools...)
	cases := []string{"redigo",
		"goredis",
		"goredis_v7",
		"goredis_v8"}
	totalOps := map[string]float64{}
	minOps := map[string]float64{
		"redigo":     50000.0,
		"goredis":    50000.0,
		"goredis_v7": 50000.0,
		"goredis_v8": 50000.0,
	}
	maxOps := map[string]float64{
		"redigo":     0.0,
		"goredis":    0.0,
		"goredis_v7": 0.0,
		"goredis_v8": 0.0,
	}
	ag := sync.WaitGroup{}
	n := 15000
	b.StartTimer()
	testCount := 10
	for i := 0; i < testCount; i++ {
		for k, v := range makeCases(3) {
			counter := int64(n) //int64(b.N)
			cur := time.Now()
			rs := New(v.pools...)
			b.ResetTimer()
			b.Run(k, func(b *testing.B) {
				for i := 0; i < runtime.GOMAXPROCS(0); i++ {
					ag.Add(1)
					go func() {
						mutex := rs.NewMutex("test-version-quorum-lock")
						for atomic.AddInt64(&counter, -1) > 0 {
							if err := mutex.Lock(); err != nil {
								atomic.AddInt64(&counter, 1)
								continue
							}
							mutex.Unlock()
						}
						ag.Done()
					}()
				}
				ag.Wait()
				//b.Log(time.Now().Sub(cur))
				//b.Log(float64(n)/(float64(time.Now().Sub(cur))/float64(time.Second)), "/OPS")
				ops := float64(n) / (float64(time.Now().Sub(cur)) / float64(time.Second))
				totalOps[k] += ops
				if minOps[k] > ops {
					minOps[k] = ops
				}
				if maxOps[k] < ops {
					maxOps[k] = ops
				}
			})
		}
	}
	fmt.Printf("%-20s\t%-15s\t%-15s\t%-15s\n", "testCase", "avgOps", "minOps", "maxOps")
	for _, i := range cases {
		fmt.Printf("%-20s\t%-15.8v\t%-15.8v\t%-15.8v\n", "Benchmark."+i, totalOps[i]/float64(testCount), minOps[i], maxOps[i])
	}
}
