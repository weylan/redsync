package redsync

import (
	"testing"
)

func BenchmarkMymutex(b *testing.B) {
	b.StopTimer()
	//lockSuccess := 0
	//lockFail := 0
	b.StartTimer()
	for k, v := range makeCases(3) {
		b.Run(k, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				go func() {
					rs := New(v.pools...)
					mutex := rs.NewMutex("test-version-quorum-lock")
					err := mutex.Lock()
					for err != nil {
						err = mutex.Lock()
					}
					mutex.Unlock()
				}()
			}

			//rs := New(v.pools...)
			//mutex := rs.NewMutex("test-version-quorum-lock")
			//err := mutex.Lock()
			//for err != nil {
			//	err = mutex.Lock()
			//}
			//mutex.Unlock()
		})
	}
	//fmt.Printf("lockSuccess:%s\tlockFail:%s\n",strconv.Itoa(lockSuccess), strconv.Itoa(lockFail))
}
