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

	driftFactor   float64
	timeoutFactor float64

	quorum int

	genValueFunc func() (string, error)
	version      int64
	until        time.Time

	pools        []redis.Pool
	successPools []redis.Pool
}

// Name returns mutex name (i.e. the Redis key).
func (m *Mutex) Name() string {
	return m.name
}

func (m *Mutex) Version() int64 {
	return m.version
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *Mutex) Until() time.Time {
	return m.until
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	return m.LockContext(nil)
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	for i := 0; i < m.tries; i++ {
		if i != 0 {
			select {
			case <-ctx.Done():
				// Exit early if the context is done.
				return ErrFailed
			case <-time.After(m.delayFunc(i)):
				// Fall-through when the delay timer completes.
			}
		}

		start := time.Now()
		if m.quorum == 1 {
			var ok bool
			var version int64
			ok, err, version = m.acquire(ctx, m.pools[0])

			now := time.Now()
			until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
			if ok && now.Before(until) {
				m.until = until
				m.version = version
				return nil
			}
		} else {
			var n int
			n, err = func() (int, error) {
				ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
				defer cancel()
				return m.actOnPoolsAsync(func(pool redis.Pool) (bool, error, int64) {
					return m.acquire(ctx, pool)
				})
			}()

			now := time.Now()
			until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
			if n >= m.quorum && now.Before(until) {
				m.until = until
				return nil
			}

			for _, p := range m.successPools {
				if p != nil {
					m.failRelease(ctx, p)
					//m.successPools[idx] = nil
					m.successPools = m.successPools[:0]
				}
			}
		}
		if i == m.tries-1 && err != nil {
			return err
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
	if m.quorum == 1 {
		return m.forceRelease(ctx, m.pools[0], m.version+1)
	} else {
		n, err := m.actOnSuccessPoolsAsync(func(pool redis.Pool) (bool, error) {
			return m.forceRelease(ctx, pool, m.version+1)
		})
		m.successPools = m.successPools[:0]
		return n >= m.quorum, err
	}
}

func (m *Mutex) UnlockVersion(version int64) (bool, error) {
	return m.UnlockVersionContext(nil, version)
}

func (m *Mutex) UnlockVersionContext(ctx context.Context, version int64) (bool, error) {
	if m.quorum == 1 {
		return m.release(ctx, m.pools[0], version)
	} else {
		n, err := m.actOnSuccessPoolsAsync(func(pool redis.Pool) (bool, error) {
			return m.release(ctx, pool, version)
		})
		return n >= m.quorum, err
	}
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend() (bool, error) {
	return m.ExtendContext(nil)
}

// ExtendContext resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) ExtendContext(ctx context.Context) (bool, error) {
	start := time.Now()
	if m.quorum == 1 {
		ok, err := m.touch(ctx, m.pools[0], int(m.expiry/time.Millisecond))
		if !ok {
			return false, err
		}
	} else {
		n, err := m.actOnSuccessPoolsAsync(func(pool redis.Pool) (bool, error) {
			return m.touch(ctx, pool, int(m.expiry/time.Millisecond))
		})
		if n < m.quorum {
			return false, err
		}
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	return false, ErrExtendFailed
}

// Valid returns true if the lock acquired through m is still valid. It may
// also return true erroneously if quorum is achieved during the call and at
// least one node then takes long enough to respond for the lock to expire.
//
// Deprecated: Use Until instead. See https://github.com/go-redsync/redsync/issues/72.
func (m *Mutex) Valid() (bool, error) {
	return m.ValidContext(nil)
}

func (m *Mutex) SetVersion(version int64) (bool, error) {
	return m.SetVersionContext(nil, version)
}

func (m *Mutex) SetVersionContext(ctx context.Context, version int64) (bool, error) {
	if m.quorum == 1 {
		return m.update(ctx, m.pools[0], version)
	} else {
		n, err := m.actOnSuccessPoolsAsync(func(pool redis.Pool) (bool, error) {
			return m.update(ctx, pool, version)
		})
		return n >= m.quorum, err
	}
}

// ValidContext returns true if the lock acquired through m is still valid. It may
// also return true erroneously if quorum is achieved during the call and at
// least one node then takes long enough to respond for the lock to expire.
//
// Deprecated: Use Until instead. See https://github.com/go-redsync/redsync/issues/72.
func (m *Mutex) ValidContext(ctx context.Context) (bool, error) {
	if m.quorum == 1 {
		return m.valid(ctx, m.pools[0])
	} else {
		n, err := m.actOnSuccessPoolsAsync(func(pool redis.Pool) (bool, error) {
			return m.valid(ctx, pool)
		})
		return n >= m.quorum, err
	}
}

func (m *Mutex) valid(ctx context.Context, pool redis.Pool) (bool, error) {
	if time.Now().After(m.until) {
		return false, nil
	}
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	reply, err := conn.HGet(m.name, "version")
	if err != nil {
		return false, err
	}
	version, err := strconv.Atoi(reply)
	if err != nil {
		return false, err
	}
	return m.version >= int64(version*-1), nil
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

	if value >= -1 * ARGV[1] then
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

func (m *Mutex) touch(ctx context.Context, pool redis.Pool, expiry int) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(touchScript, m.name, int(m.version), expiry)
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
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

	if value >= -1 * ARGV[1] then
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

func (m *Mutex) update(ctx context.Context, pool redis.Pool, newVersion int64) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(updateScript, m.name, int(m.version), int(newVersion))
	if err != nil {
		return false, err
	}
	if status != 0 {
		m.version = newVersion
	}
	return status != int64(0), nil
}

func (m *Mutex) actOnPoolsAsync(actFn func(redis.Pool) (bool, error, int64)) (int, error) {
	type result struct {
		Node    int
		Status  bool
		Err     error
		pool    redis.Pool
		version int64
	}

	ch := make(chan result)
	for node, pool := range m.pools {
		go func(node int, pool redis.Pool) {
			r := result{Node: node}
			r.Status, r.Err, r.version = actFn(pool)
			r.pool = pool
			ch <- r
		}(node, pool)
	}
	n := 0
	var taken []int
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
			err = multierror.Append(err, &RedisError{Node: r.Node, Err: r.Err})
		} else {
			taken = append(taken, r.Node)
			err = multierror.Append(err, &ErrNodeTaken{Node: r.Node})
		}
	}

	if len(taken) >= m.quorum {
		return n, &ErrTaken{Nodes: taken}
	}
	return n, err
}

func (m *Mutex) actOnSuccessPoolsAsync(actFn func(redis.Pool) (bool, error)) (int, error) {
	type result struct {
		Node    int
		Status  bool
		Err     error
		pool    redis.Pool
		version int64
	}

	ch := make(chan result)
	for node, pool := range m.successPools {
		if pool != nil {
			go func(node int, pool redis.Pool) {
				r := result{Node: node}
				r.Status, r.Err = actFn(pool)
				r.pool = pool
				ch <- r
			}(node, pool)
		}
	}
	n := 0
	var taken []int
	var err error
	for range m.successPools {
		r := <-ch
		if r.Status {
			n++
		} else if r.Err != nil {
			err = multierror.Append(err, &RedisError{Node: r.Node, Err: r.Err})
		} else {
			taken = append(taken, r.Node)
			err = multierror.Append(err, &ErrNodeTaken{Node: r.Node})
		}
	}

	if len(taken) >= m.quorum {
		return n, &ErrTaken{Nodes: taken}
	}
	return n, err
}
