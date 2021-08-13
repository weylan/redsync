package redsync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/weylan/redsync/redis"
)

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	name   string
	expiry time.Duration

	tries     int
	delayFunc DelayFunc

	factor float64

	quorum int

	genValueFunc func() (string, error)
	version      int64
	until        time.Time

	pools        []redis.Pool
	successPools []*redis.Pool
}

// Name returns mutex name (i.e. the Redis key).
func (m *Mutex) Name() string {
	return m.name
}

func (m *Mutex) Version() int64 {
	return m.version
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	return m.LockContext(nil)
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) LockContext(ctx context.Context) error {
	for i := 0; i < m.tries; i++ {
		if i != 0 {
			time.Sleep(m.delayFunc(i))
		}

		start := time.Now()
		n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error, int64) {
			return m.acquire(ctx, pool)
		})
		if n == 0 && err != nil {
			return err
		}

		now := time.Now()
		until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.factor)))
		if n >= m.quorum && now.Before(until) {
			m.until = until
			//println("n:", n, "\t m.successPools's length:", len(m.successPools))
			return nil
		}

		//_, _ = m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		//	return m.release(ctx, pool, m.version)
		//})
		for _, p := range m.successPools {
			if p != nil {
				m.failRelease(ctx, *p)
				//m.successPools[idx] = nil
				m.successPools = m.successPools[0:0]
			}
		}
	}
	return ErrFailed
}

// Unlock unlocks m and returns the status of unlock.
func (m *Mutex) Unlock() (bool, error) {
	return m.UnlockContext(nil)
}

// UnlockContext unlocks m and returns the status of unlock.
func (m *Mutex) UnlockContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsyncForVersionUnlock(func(pool redis.Pool) (bool, error) {
		return m.forceRelease(ctx, pool, m.version+1)
	})
	m.successPools = m.successPools[0:0]
	return n >= m.quorum, err
}

func (m *Mutex) UnlockVersion(version int64) (bool, error) {
	return m.UnlockVersionContext(nil, version)
}

func (m *Mutex) UnlockVersionContext(ctx context.Context, version int64) (bool, error) {
	n, err := m.actOnPoolsAsyncForVersionUnlock(func(pool redis.Pool) (bool, error) {
		return m.release(ctx, pool, m.version+1)
	})
	return n >= m.quorum, err
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend() (bool, error) {
	return m.ExtendContext(nil)
}

// ExtendContext resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) ExtendContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error, int64) {
		return m.touch(ctx, pool, int(m.expiry/time.Millisecond))
	})
	return n >= m.quorum, err
}

func (m *Mutex) Valid() (bool, error) {
	return m.ValidContext(nil)
}

func (m *Mutex) SetVersion(version int64) (bool, error) {
	return m.SetVersionContext(nil, version)
}

func (m *Mutex) SetVersionContext(ctx context.Context, version int64) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error, int64) {
		return m.update(ctx, pool, version)
	})
	return n >= m.quorum, err
}

func (m *Mutex) ValidContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error, int64) {
		return m.valid(ctx, pool)
	})
	return n >= m.quorum, err
}

func (m *Mutex) valid(ctx context.Context, pool redis.Pool) (bool, error, int64) {
	if time.Now().After(m.until) {
		return false, nil, 0
	}
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err, 0
	}
	defer conn.Close()
	reply, err := conn.HGet(m.name, "version")
	if err != nil {
		return false, err, 0
	}
	version, err := strconv.Atoi(reply)
	if err != nil {
		return false, err, 0
	}
	return m.version == int64(version*-1), nil, 0
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

var lockScript = redis.NewScript(1, `
	local key = KEYS[1]
	local result = redis.call("HMGET", key, "version", "expire")
	local t = redis.call("time")
	local cur = t[1] * 1000 + t[2] / 1000
	local value = result[1]
	local expire = result[2]

	if type(value) == "string" then
		value = tonumber(value)
	end

	if type(expire) == "string" then
		expire = tonumber(expire)
	end

	if value and value <= 0 and cur <= expire then
		return 0
	end

	if not value or value == 0 then
		value = 1
	elseif value <= 0 then
		value = value * -1 + 1
	end

	expire = ARGV[1] + cur
	redis.call("HMSET", key, "version", value * -1, "expire", expire)
	return value
`)

func (m *Mutex) acquire(ctx context.Context, pool redis.Pool) (bool, error, int64) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err, 0
	}
	defer conn.Close()
	status, err := conn.Eval(lockScript, m.name, int(m.expiry/time.Millisecond))
	if err != nil {
		return false, err, 0
	}
	version, _ := status.(int64)
	//if version, ok := status.(int64); ok && version != 0 {
	//	if version >= m.version {
	//		m.version = version
	//	}
	//}
	return status != int64(0), nil, version
}

var releaseScript = redis.NewScript(1, `
	local key = KEYS[1]
	local result = redis.call("HMGET", key, "version", "expire")
	local value = result[1]
	local expire = result[2]
	
	if not value then
		return 0
	end

	if type(value) == "string" then
		value = tonumber(value)
	end

	if value >= 0 then
		return 0
	end

	if value == -1 * ARGV[1] then
		value = ARGV[2]
	else
		return 0
	end

	redis.call("HMSET", key, "version", value, "expire", 0)
	return 1
`)

func (m *Mutex) release(ctx context.Context, pool redis.Pool, newVersion int64) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(releaseScript, m.name, int(m.version), int(newVersion))
	if err != nil {
		return false, err
	}
	if status != 0 {
		m.version = newVersion
	}
	return status != 0, nil
}

var forceReleaseScript = redis.NewScript(1, `
	local key = KEYS[1]
	local result = redis.call("HMGET", key, "version", "expire")
	local value = result[1]
	local expire = result[2]
	
	if not value then
		return 0
	end

	if type(value) == "string" then
		value = tonumber(value)
	end

	if value >= 0 then
		return 0
	end

	if value >= -1 * ARGV[1] then
		value = ARGV[2]
	else
		return 0
	end

	redis.call("HMSET", key, "version", value, "expire", 0)
	return 1
`)

func (m *Mutex) forceRelease(ctx context.Context, pool redis.Pool, newVersion int64) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(forceReleaseScript, m.name, int(m.version), int(newVersion))
	if err != nil {
		return false, err
	}
	if status != 0 {
		m.version = newVersion
	}
	return status != 0, nil
}

var failReleaseScript = redis.NewScript(1, `
	local key = KEYS[1]
	local result = redis.call("HMGET", key, "version", "expire")
	local value = result[1]
	local expire = result[2]
	
	if not value then
		return 0
	end

	if type(value) == "string" then
		value = tonumber(value)
	end

	if value >= 0 then
		return 0
	end
	
	value = -1 * value

	redis.call("HMSET", key, "version", value, "expire", 0)
	return 1
`)

func (m *Mutex) failRelease(ctx context.Context, pool redis.Pool) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(failReleaseScript, m.name)
	if err != nil {
		return false, err
	}
	if status != 0 {

	}
	return status != 0, nil
}

var touchScript = redis.NewScript(1, `
	local key = KEYS[1]
	local result = redis.call("HMGET", key, "version", "expire")
	local value = result[1]
	local expire = result[2]
	
	if type(value) == "string" then
		value = tonumber(value)
	end

	if type(expire) == "string" then
		expire = tonumber(expire)
	end

	if value == -1 * ARGV[1] then
		local t = redis.call("time")
		local cur = t[1] * 1000 + t[2] / 1000
		if cur > expire then
			return 0
		end
		redis.call("HSET", key, "expire", cur + ARGV[2])
		return value * -1
	else
		return 0
	end
`)

func (m *Mutex) touch(ctx context.Context, pool redis.Pool, expiry int) (bool, error, int64) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err, 0
	}
	defer conn.Close()
	status, err := conn.Eval(touchScript, m.name, int(m.version), expiry)
	if err != nil {
		return false, err, 0
	}
	return status != int64(0), nil, 0
}

var updateScript = redis.NewScript(1, `
	local key = KEYS[1]
	local result = redis.call("HMGET", key, "version", "expire")
	local value = result[1]
	local expire = result[2]
	
	if type(value) == "string" then
		value = tonumber(value)
	end
	
	if type(expire) == "string" then
		expire = tonumber(expire)
	end

	if value == -1 * ARGV[1] then
		local t = redis.call("time")
		local cur = t[1] * 1000 + t[2] / 1000
		if cur > expire then
			return 0
		end
		redis.call("HSET", key, "version", -1 * ARGV[2])
		return value * -1
	else
		return 0
	end
`)

func (m *Mutex) update(ctx context.Context, pool redis.Pool, newVersion int64) (bool, error, int64) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err, 0
	}
	defer conn.Close()
	status, err := conn.Eval(updateScript, m.name, int(m.version), int(newVersion))
	if err != nil {
		return false, err, 0
	}
	if status != 0 {
		m.version = newVersion
	}
	return status != int64(0), nil, 0
}

func (m *Mutex) actOnPoolsAsync(actFn func(redis.Pool) (bool, error, int64)) (int, error) {
	type result struct {
		Status  bool
		Err     error
		pool    *redis.Pool
		version int64
	}

	ch := make(chan result)
	for _, pool := range m.pools {
		go func(pool redis.Pool) {
			r := result{}
			r.Status, r.Err, r.version = actFn(pool)
			r.pool = &pool
			//if r.Status {
			//	m.successPools = append(m.successPools, &pool)
			//}
			ch <- r
		}(pool)
	}
	n := 0
	var err error
	for range m.pools {
		r := <-ch
		if r.Status {
			n++
			m.successPools = append(m.successPools, r.pool)
			if r.version >= m.version {
				m.version = r.version
			}
		} else if r.Err != nil {
			err = multierror.Append(err, r.Err)
		}
	}
	return n, err
}

func (m *Mutex) actOnPoolsAsyncForVersionUnlock(actFn func(redis.Pool) (bool, error)) (int, error) {
	type result struct {
		Status bool
		Err    error
	}

	ch := make(chan result)
	for _, pool := range m.successPools {
		if pool != nil {
			go func(pool redis.Pool) {
				r := result{}
				r.Status, r.Err = actFn(pool)
				ch <- r
			}(*pool)
		}
	}
	n := 0
	var err error
	for range m.successPools {
		r := <-ch
		if r.Status {
			n++
		} else if r.Err != nil {
			err = multierror.Append(err, r.Err)
		}
	}
	return n, err
}
