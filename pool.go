/*
The MIT License (MIT)

Copyright (c) 2013 Fatih Arslan

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package faktory_worker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")
)

// Closeable interface describes a closable implementation.  The underlying
// procedure of the Close() function is determined by its implementation
type Closeable interface {
	// Close closes the object
	Close() error
}

// Pool interface describes a pool implementation. A pool should have maximum
// capacity. An ideal pool is threadsafe and easy to use.
type Pool interface {
	// Get returns a new connection from the pool. Closing the connections puts
	// it back to the Pool. Closing it when the pool is destroyed or full will
	// be counted as an error.
	Get() (Closeable, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()

	// Len returns the current number of connections of the pool.
	Len() int
}

// PoolConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type PoolConn struct {
	Closeable
	mu       sync.RWMutex
	c        *channelPool
	unusable bool
}

// Close puts the given connects back to the pool instead of closing it.
func (p *PoolConn) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		p.c.mu.Lock()
		p.c.connsToOpen++
		p.c.mu.Unlock()

		if p.Closeable != nil {
			return p.Closeable.Close()
		}
		return nil
	}
	return p.c.put(p.Closeable)
}

// MarkUnusable marks the connection not usable any more, to let the pool close
// it instead of returning it to pool.
func (p *PoolConn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

// wrapConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn Closeable) Closeable {
	p := &PoolConn{c: c}
	p.Closeable = conn
	return p
}

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.Mutex
	conns chan Closeable

	//connsToOpen reprents the number ot connections that can still be opened
	// before reaching the maximum capacity.
	connsToOpen int

	// net.Conn generator
	factory Factory
}

// Factory is a function to create new connections.
type Factory func() (Closeable, error)

// NewChannelPool returns a new pool based on buffered channels with an initial
// capacity and maximum capacity. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool, a new connection will be created via the Factory()
// method.
func NewChannelPool(capacity int, factory Factory) (Pool, error) {
	if capacity <= 0 {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		conns:   make(chan Closeable, capacity),
		factory: factory,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < capacity; i++ {
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- conn
	}

	go reportStats(c)

	return c, nil
}

func (c *channelPool) getConns() chan Closeable {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get() (Closeable, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	for {
		// wrap our connections with out custom net.Conn implementation (wrapConn
		// method) that puts the connection back to the pool if it's closed.
		select {
		case conn := <-conns:
			if conn == nil {
				c.mu.Lock()
				c.connsToOpen--
				c.mu.Unlock()

				return nil, ErrClosed
			}

			return c.wrapConn(conn), nil

		case <-time.After(1 * time.Second):
			// We only open a new connection if we haven't reached the capacity
			// of the queue. If we have, we wait a bit and block again for a
			// connection.
			c.mu.Lock()
			if c.connsToOpen > 0 {
				c.connsToOpen--
				c.mu.Unlock()

				conn, err := c.factory()
				if err != nil {
					c.mu.Lock()
					c.connsToOpen++
					c.mu.Unlock()

					return nil, err
				}

				return c.wrapConn(conn), nil
			}
			c.mu.Unlock()

			time.Sleep(time.Second)
		}
	}

}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(conn Closeable) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, block until it
	// is possible to return the connection.
	c.conns <- conn
	return nil
}

// Close shuts down all of the Closeable objects in the Pool
func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

// Len returns the number of Closeable objects in the Pool
func (c *channelPool) Len() int { return len(c.getConns()) }

// reportStats prints every minute stats regarding the pool.
func reportStats(c *channelPool) {
	for {
		select {
		case <-time.NewTicker(time.Minute).C:
			c.mu.Lock()
			fmt.Printf("Faktory client pool: Available connections: %v, to open: %v\n",
				len(c.conns), c.connsToOpen)
			c.mu.Unlock()
		}
	}
}
