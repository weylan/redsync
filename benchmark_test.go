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
	totalSOT := map[string]int{}
	minSOT := map[string]int{
		"redigo":     50000000000,
		"goredis":    50000000000,
		"goredis_v7": 50000000000,
		"goredis_v8": 50000000000,
	}
	maxSOT := map[string]int{
		"redigo":     0,
		"goredis":    0,
		"goredis_v7": 0,
		"goredis_v8": 0,
	}
	staticSOT := map[string]map[int]int{
		"redigo":     {},
		"goredis":    {},
		"goredis_v7": {},
		"goredis_v8": {},
	}
	ag := sync.WaitGroup{}
	n := 5000
	b.StartTimer()
	testCount := 5
	poolCount := 3
	for i := 0; i < testCount; i++ {
		for k, v := range makeCases(poolCount) {
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
							sinStart := time.Now()
							if err := mutex.Lock(); err != nil {
								atomic.AddInt64(&counter, 1)
								continue
							}
							mutex.Unlock()
							sinT := time.Now().Sub(sinStart)
							totalSOT[k] += int(sinT)
							if int(sinT) < minSOT[k] {
								minSOT[k] = int(sinT)
							}
							if int(sinT) > maxSOT[k] {
								maxSOT[k] = int(sinT)
							}
							if int(sinT)/1e8 >= 9 {
								staticSOT[k][9]++
							} else {
								staticSOT[k][int(sinT)/1e8]++
							}
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
	fmt.Printf("%s|%s|%s|%s|%s|%s|%s\n",
		"testCase", "avgOps", "minOps", "maxOps", "avgSOT", "minSOT", "maxSOT")
	for _, i := range cases {
		fmt.Printf("%s|%v|%v|%v|%v|%v|%v\n",
			"Benchmark."+i, totalOps[i]/float64(testCount), minOps[i], maxOps[i],
			totalSOT[i]/(n*testCount), minSOT[i], maxSOT[i])
	}
	//fmt.Println(staticSOT)
	//for _, i := range cases {
	//	p := plot.New()
	//	p.Title.Text = i+"_"+strconv.Itoa(poolCount)
	//	bins := plotter.XYs{
	//		//{0.0, float64(staticSOT[i][0])},
	//		//{0.1, float64(staticSOT[i][1])},
	//		{0.2, float64(staticSOT[i][2])},
	//		{0.3, float64(staticSOT[i][3])},
	//		{0.4, float64(staticSOT[i][4])},
	//		{0.5, float64(staticSOT[i][5])},
	//		{0.6, float64(staticSOT[i][6])},
	//		{0.7, float64(staticSOT[i][7])},
	//		{0.8, float64(staticSOT[i][8])},
	//		{0.9, float64(staticSOT[i][9])},
	//	}
	//	h, _ := plotter.NewHistogram(bins, 10)
	//	p.Add(h)
	//	p.Save(2*vg.Inch, 2*vg.Inch, i+"Histogram_"+strconv.Itoa(poolCount)+".png")
	//}
}
