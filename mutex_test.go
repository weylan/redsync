package redsync

import (
	"fmt"
	goredislib "github.com/go-redis/redis"
	"github.com/weylan/redsync/redis/goredis"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/weylan/redsync/redis"
)

func TestT(t *testing.T) {
	//for {
	//	TestMutex(t)
	//}
	client := goredislib.NewClient(&goredislib.Options{
		Addr: "10.7.69.238:6379",
	})
	pool := goredis.NewPool(client) // or, pool := redigo.NewPool(...)

	// Create an instance of redisync to be used to obtain a mutual exclusion
	// lock.
	rs := New(pool)

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	mutexname := "my-global-mutex"
	mutex := rs.NewMutex(mutexname)

	// Obtain a lock for our given mutex. After this is successful, no one else
	// can obtain the same lock (the same mutex name) until we unlock it.
	if err := mutex.Lock(); err != nil {
		panic(err)
	}

	// Do your work that requires the lock.

	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := mutex.Unlock(); !ok || err != nil {
		panic("unlock failed")
	}
}

func TestMutex(t *testing.T) {
	for k, v := range makeCases(3) {
		t.Run(k, func(t *testing.T) {
			mutexes := newTestMutexes(v.pools, "test-mutex", v.poolCount)
			orderCh := make(chan int)
			for i, mutex := range mutexes {
				go func(i int, mutex *Mutex) {
					err := mutex.Lock()
					if err != nil {
						t.Fatalf("mutex lock failed: %s", err)
					}
					defer mutex.Unlock()

					assertAcquiredVersion(t, v.pools, mutex)

					orderCh <- i
				}(i, mutex)
			}
			for range mutexes {
				<-orderCh
			}
		})
	}
}

func TestMutexAlreadyLocked(t *testing.T) {
	for k, v := range makeCases(4) {
		t.Run(k, func(t *testing.T) {
			rs := New(v.pools...)
			key := "test-lock"

			mutex1 := rs.NewMutex(key)
			err := mutex1.Lock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			assertAcquired(t, v.pools, mutex1)

			mutex2 := rs.NewMutex(key)
			err = mutex2.Lock()
			var errTaken *ErrTaken
			if !errors.As(err, &errTaken) {
				t.Fatalf("mutex was not already locked: %s", err)
			}
		})
	}
}

func TestMutexExtend(t *testing.T) {
	for k, v := range makeCases(1) {
		t.Run(k, func(t *testing.T) {
			mutexes := newTestMutexes(v.pools, "test-mutex-extend", 1)
			mutex := mutexes[0]

			err := mutex.Lock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			defer mutex.Unlock()

			time.Sleep(1 * time.Second)

			expiries := getPoolExpiries(v.pools, mutex.name)
			ok, err := mutex.Extend()
			if err != nil {
				t.Fatalf("mutex extend failed: %s", err)
			}
			if !ok {
				t.Fatalf("Expected a valid mutex")
			}
			expiries2 := getPoolExpiries(v.pools, mutex.name)

			for i, expiry := range expiries {
				if expiry > expiries2[i] {
					t.Fatalf("Expected expiries[%d] >= %d, got %d", i, expiry, expiries2[i])
				}
			}
		})
	}
}

func TestMutexExtendExpired(t *testing.T) {
	for k, v := range makeCases(3) {
		t.Run(k, func(t *testing.T) {
			mutexes := newTestMutexes(v.pools, "test-mutex-extend", 1)
			mutex := mutexes[0]
			mutex.expiry = 500 * time.Millisecond

			err := mutex.Lock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			defer mutex.Unlock()

			time.Sleep(2 * time.Second)

			ok, err := mutex.Extend()
			if err == nil {
				t.Fatalf("mutex extend didn't fail")
			}
			if ok {
				t.Fatalf("Expected ok == false, got %v", ok)
			}
		})
	}
}

func TestMutexVersionExpired(t *testing.T) {
	for k, v := range makeCases(3) {
		t.Run(k, func(t *testing.T) {
			mutexes := newTestMutexes(v.pools, "test-mutex", 1)
			mutex := mutexes[0]
			err := mutex.Lock()
			if err != nil {
				return
			}
			oldVersion := mutex.Version()
			fmt.Println(oldVersion)
			time.Sleep(10 * time.Second)
			mutex.Lock()
			//mutex.Unlock()
			fmt.Println(mutex.version, " ", oldVersion)
			if oldVersion+1 != mutex.Version() {
				t.Fatalf("version not update")
			}
		})
	}
}

func TestMutexUnlockExpired(t *testing.T) {
	for k, v := range makeCases(3) {
		t.Run(k, func(t *testing.T) {
			mutexes := newTestMutexes(v.pools, "test-mutex-extend", 1)
			mutex := mutexes[0]
			mutex.expiry = time.Second

			err := mutex.Lock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			defer mutex.Unlock()

			time.Sleep(2 * time.Second)

			ok, err := mutex.Unlock()
			if err != nil {
				t.Fatalf("mutex unlock failed: %s", err)
			}
			if !ok {
				t.Fatalf("Expected ok == true, got %v", ok)
			}
			ok, err = mutex.Unlock()
			if err != nil {
				t.Fatalf("mutex unlock failed: %s", err)
			}
			if ok {
				t.Fatalf("Excepted ok == false got %v", ok)
			}
		})
	}
}

func TestMutexQuorum(t *testing.T) {
	//for k, v := range makeCases(4) {
	//	t.Run(k, func(t *testing.T) {
	//		for mask := 0; mask < 1<<uint(len(v.pools)); mask++ {
	//			mutexes := newTestMutexes(v.pools, "test-mutex-partial-"+strconv.Itoa(mask), 1)
	//			mutex := mutexes[0]
	//			mutex.tries = 1
	//
	//			n := clogPools(v.pools, mask, mutex)
	//
	//			if n >= len(v.pools)/2+1 {
	//				err := mutex.Lock()
	//				if err != nil {
	//					t.Fatalf("mutex lock failed: %s", err)
	//				}
	//				assertAcquiredVersion(t, v.pools, mutex)
	//				//assertAcquired(t, v.pools, mutex)
	//			} else {
	//				err := mutex.Lock()
	//				if errors.Is(err, &ErrNodeTaken{}) {
	//					t.Fatalf("Expected err == %q, got %q", ErrNodeTaken{}, err)
	//				}
	//			}
	//		}
	//	})
	//}
}

func TestValid(t *testing.T) {
	for k, v := range makeCases(4) {
		t.Run(k, func(t *testing.T) {
			rs := New(v.pools...)
			key := "test-shared-lock"

			mutex1 := rs.NewMutex(key, WithExpiry(time.Hour))
			err := mutex1.Lock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			assertAcquiredVersion(t, v.pools, mutex1)

			ok, err := mutex1.Valid()
			if err != nil {
				t.Fatalf("mutex valid failed: %s", err)
			}
			if !ok {
				t.Fatalf("Expected a valid mutex")
			}

			mutex2 := rs.NewMutex(key)
			err = mutex2.Lock()
			if err == nil {
				t.Fatalf("mutex need lock failed: %d m1:%d", mutex2.version, mutex1.version)
			}
		})
	}
}

func TestSetVersion(t *testing.T) {
	for k, v := range makeCases(4) {
		t.Run(k, func(t *testing.T) {
			rs := New(v.pools...)
			key := "test-shared-lock-version"

			mutex1 := rs.NewMutex(key, WithExpiry(time.Hour))
			err := mutex1.Lock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			defer mutex1.Unlock()
			assertAcquiredVersion(t, v.pools, mutex1)

			ok, err := mutex1.SetVersion(100)
			if !ok || err != nil || mutex1.Version() != 100 {
				t.Fatalf("mutex set version failed: %s", err)
			}
			assertAcquiredVersion(t, v.pools, mutex1)
		})
	}
}

func TestMutexLockUnlockSplit(t *testing.T) {
	for k, v := range makeCases(3) {
		t.Run(k, func(t *testing.T) {
			rs := New(v.pools...)
			key := "test-split-lock-0"

			mutex1 := rs.NewMutex(key, WithExpiry(time.Hour))
			err := mutex1.Lock()
			defer mutex1.Unlock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			assertAcquiredVersion(t, v.pools, mutex1)

			mutex2 := rs.NewMutex(key, WithExpiry(time.Hour), WithVersion(mutex1.version))
			ok, err := mutex2.Unlock()
			if err != nil {
				t.Fatalf("mutex unlock failed: %s", err)
			}
			if ok {
				t.Fatalf("Expected a invalid mutex")
			}
		})
	}
}

func TestMutexLockUnlockVersion(t *testing.T) {
	for k, v := range makeCases(4) {
		t.Run(k, func(t *testing.T) {
			rs := New(v.pools...)
			key := "test-release-lock-verison-0"

			mutex1 := rs.NewMutex(key, WithExpiry(time.Hour))
			err := mutex1.Lock()
			defer mutex1.Unlock()
			if err != nil {
				t.Fatalf("mutex lock failed: %s", err)
			}
			assertAcquiredVersion(t, v.pools, mutex1)

			mutex2 := rs.NewMutex(key, WithExpiry(time.Hour), WithVersion(mutex1.version))
			ok, err := mutex2.UnlockVersion(100)
			if err != nil {
				t.Fatalf("mutex unlock failed: %s", err)
			}
			if ok {
				t.Fatalf("Expected a invalid mutex")
			}
			ok, err = mutex1.SetVersion(100)
			if err != nil {
				t.Fatalf("mutex unlock failed: %s", err)
			}
			if !ok {
				t.Fatalf("Expected true")
			}
			//mutex2.version *= -1
			//assertAcquiredVersion(t, v.pools, mutex2)
		})
	}
}

func getPoolValues(pools []redis.Pool, name string) []string {
	values := make([]string, len(pools))
	for i, pool := range pools {
		conn, err := pool.Get(nil)
		if err != nil {
			panic(err)
		}
		value, err := conn.HGet(name, "version")
		if err != nil {
			panic(err)
		}
		_ = conn.Close()
		values[i] = value
	}
	return values
}

func getPoolExpiries(pools []redis.Pool, name string) []int {
	expiries := make([]int, len(pools))
	for i, pool := range pools {
		conn, err := pool.Get(nil)
		if err != nil {
			panic(err)
		}
		expiry, err := conn.HGet(name, "expire")
		if err != nil {
			panic(err)
		}
		_ = conn.Close()
		expiries[i], _ = strconv.Atoi(expiry)
	}
	return expiries
}

func clogPools(pools []redis.Pool, mask int, mutex *Mutex) int {
	n := 0
	for i, pool := range pools {
		if mask&(1<<uint(i)) == 0 {
			n++
			continue
		}
		conn, err := pool.Get(nil)
		if err != nil {
			panic(err)
		}
		//_, err = conn.Set(mutex.name, "foobar")
		if err != nil {
			panic(err)
		}
		_ = conn.Close()
	}
	return n
}

func newTestMutexes(pools []redis.Pool, name string, n int) []*Mutex {
	mutexes := make([]*Mutex, n)
	for i := 0; i < n; i++ {
		mutexes[i] = &Mutex{
			name:          name + "-" + strconv.Itoa(i),
			expiry:        8 * time.Second,
			tries:         32,
			delayFunc:     func(tries int) time.Duration { return 500 * time.Millisecond },
			genValueFunc:  nil,
			driftFactor:   0.01,
			timeoutFactor: 0.05,
			quorum:       len(pools)/2 + 1,
			pools:        pools,
			successPools: make([]redis.Pool, 0),
		}
	}
	return mutexes
}

func assertAcquired(t *testing.T, pools []redis.Pool, mutex *Mutex) {
	n := 0
	values := getPoolValues(pools, mutex.name)
	for _, value := range values {
		if value == strconv.Itoa(int(mutex.version)*-1) {
			n++
		}
	}
	if n < mutex.quorum {
		t.Fatalf("Expected n >= %d, got %d", mutex.quorum, n)
	}
}

func getPoolVersion(pools []redis.Pool, name string) []string {
	values := make([]string, len(pools))
	for i, pool := range pools {
		if pool != nil {
			conn, err := pool.Get(nil)
			if err != nil {
				panic(err)
			}
			value, err := conn.HGet(name, "version")
			if err != nil {
				panic(err)
			}
			_ = conn.Close()
			values[i] = value
		}
	}
	return values
}

func assertAcquiredVersion(t *testing.T, pools []redis.Pool, mutex *Mutex) {
	n := 0
	values := getPoolVersion(mutex.successPools, mutex.name)
	for _, value := range values {
		v, _ := strconv.Atoi(value)
		if v < 0 && v >= int(mutex.version)*-1 {
			n++
		}
	}
	if n < mutex.quorum {
		t.Fatalf("Expected n >= %d, got %d", mutex.quorum, n)
	}
}
