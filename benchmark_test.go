package redsync

import (
	"testing"
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
	v := cases["goredis_v8"]
	rs := New(v.pools...)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			mutex := rs.NewMutex("test-version-quorum-lock")
			err := mutex.Lock()
			for err != nil {
				err = mutex.Lock()
			}
			mutex.Unlock()
		}()
	}

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
