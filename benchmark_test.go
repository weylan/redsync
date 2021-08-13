package redsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkMymutex(b *testing.B) {
	b.StopTimer()
	//lockSuccess := 0
	//lockFail := 0

	cases := makeCases(3)
	//		"redigo"
	//		"goredis"
	//		"goredis_v7"
	//		"goredis_v8"
	v := cases["goredis_v7"]
	rs := New(v.pools...)
	ag := sync.WaitGroup{}
	counter := int64(10000) //int64(b.N)
	b.StartTimer()
	cur := time.Now()
	//for i := 0; i < b.N; i++ {
	//	go func() {
	//		mutex := rs.NewMutex("test-version-quorum-lock")
	//		err := mutex.Lock()
	//		for err != nil {
	//
	//		}
	//		mutex.Unlock()
	//	}()
	//}

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

	b.Log(time.Now().Sub(cur))
	//for k, v := range makeCases(3) {
	//	b.Run(k, func(b *testing.B) {
	//		rs := New(v.pools...)
	//		b.ResetTimer()
	//		for i := 0; i < b.N; i++ {
	//			go func() {
	//				mutex := rs.NewMutex("test-version-quorum-lock")
	//				err := mutex.Lock()
	//				for err != nil {
	//					err = mutex.Lock()
	//				}
	//				mutex.Unlock()
	//			}()
	//		}

	//rs := New(v.pools...)
	//mutex := rs.NewMutex("test-version-quorum-lock")
	//err := mutex.Lock()
	//for err != nil {
	//	err = mutex.Lock()
	//}
	//mutex.Unlock()

	//})
	//}
}
