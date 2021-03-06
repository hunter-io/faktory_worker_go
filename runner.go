package faktory_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	faktory "github.com/hunter-io/faktory/client"
)

type eventType int

const (
	Startup  eventType = 1
	Quiet    eventType = 2
	Shutdown eventType = 3
)

// Register registers a handler for the given jobtype.  It is expected that all jobtypes
// are registered upon process startup.
//
// faktory_worker.Register("ImportantJob", ImportantFunc)
func (mgr *Manager) Register(name string, fn Perform) {
	mgr.jobHandlers[name] = func(ctx Context, job *faktory.Job) error {
		return fn(ctx, job.Args...)
	}
}

// Manager coordinates the processes for the worker.  It is responsible for
// starting and stopping goroutines to perform work at the desired concurrency level
type Manager struct {
	Concurrency int
	Pool
	Logger Logger

	// ConcurrencyLimits stores the maximum number of jobs per queue a worker
	// should process at any time.
	ConcurrencyLimits map[string]int

	queues     []string
	middleware []MiddlewareFunc
	quiet      bool
	// The done channel will always block unless
	// the system is shutting down.
	done           chan interface{}
	shutdownWaiter *sync.WaitGroup
	jobHandlers    map[string]Handler
	eventHandlers  map[eventType][]func()

	activityPerQueue map[string]int
	activityMutex    sync.Mutex

	// This only needs to be computed once. Store it here to keep things fast.
	weightedPriorityQueuesEnabled bool
	weightedQueues                map[string]int

	// context isn't available to the caller but is managed internally to let
	// jobs stop their work when Faktory receives a shutdown signal.
	context context.Context
}

// Register a callback to be fired when a process lifecycle event occurs.
// These are useful for hooking into process startup or shutdown.
func (mgr *Manager) On(event eventType, fn func()) {
	mgr.eventHandlers[event] = append(mgr.eventHandlers[event], fn)
}

// After calling Quiet(), no more jobs will be pulled
// from Faktory by this process.
func (mgr *Manager) Quiet() {
	mgr.Logger.Info("Quieting...")
	mgr.quiet = true
	mgr.fireEvent(Quiet)
}

// Terminate signals that the various components should shutdown.
// Blocks on the shutdownWaiter until all components have finished.
func (mgr *Manager) Terminate() {
	mgr.Logger.Info("Shutting down...")
	close(mgr.done)
	mgr.fireEvent(Shutdown)
	mgr.shutdownWaiter.Wait()
	mgr.Pool.Close()
	mgr.Logger.Info("Goodbye")
	os.Exit(0)
}

// Use adds middleware to the chain.
func (mgr *Manager) Use(middleware ...MiddlewareFunc) {
	mgr.middleware = append(mgr.middleware, middleware...)
}

// NewManager returns a new manager with default values.
func NewManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	mgr := &Manager{
		Concurrency: 20,
		Logger:      NewStdLogger(),

		queues:         []string{"default"},
		done:           make(chan interface{}),
		shutdownWaiter: &sync.WaitGroup{},
		jobHandlers:    map[string]Handler{},
		eventHandlers: map[eventType][]func(){
			Startup:  []func(){},
			Quiet:    []func(){},
			Shutdown: []func(){},
		},

		ConcurrencyLimits: make(map[string]int),
		activityPerQueue:  make(map[string]int),

		weightedPriorityQueuesEnabled: false,
		weightedQueues:                make(map[string]int),
		context:                       ctx,
	}

	go func() {
		sigchan := hookSignals()

		<-sigchan

		// We wait a short amount of time to ensure the manager will stop
		// fetching new jobs before the cancel function is sent out.
		// This is important in case one of the jobs pushes itself back in the
		// queue as otherwise, it will risk being pulled again by the same
		// manager.
		time.Sleep(time.Millisecond * 200)

		mgr.Logger.Info("Received signal, canceling context")

		cancel()
	}()

	return mgr
}

// Run starts processing jobs.
// This method does not return.
func (mgr *Manager) Run() {
	// This will signal to Faktory that all connections from this process
	// are worker connections.
	// If the WID is already defined, do not change it.
	if faktory.RandomProcessWid == "" {
		rand.Seed(time.Now().UnixNano())

		faktory.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)
	}

	if mgr.Pool == nil {
		pool, err := NewChannelPool(mgr.Concurrency, func() (Closeable, error) { return faktory.Open() })
		if err != nil {
			panic(err)
		}
		mgr.Pool = pool
	}

	mgr.fireEvent(Startup)

	go heartbeat(mgr)

	for i := 0; i < mgr.Concurrency; i++ {
		go process(mgr, i)
	}

	sigchan := hookSignals()

	for {
		sig := <-sigchan
		handleEvent(signalMap[sig], mgr)
	}
}

// One of the Process*Queues methods should be called once before Run()
func (mgr *Manager) ProcessStrictPriorityQueues(queues ...string) {
	mgr.queues = queues
	mgr.weightedPriorityQueuesEnabled = false
}

func (mgr *Manager) ProcessWeightedPriorityQueues(queues map[string]int) {
	uniqueQueues := queueKeys(queues)

	mgr.queues = uniqueQueues
	mgr.weightedQueues = queues
	mgr.weightedPriorityQueuesEnabled = true
}

func (mgr *Manager) queueList() []string {
	if mgr.weightedPriorityQueuesEnabled {

		// A random number is generated for each weighted queue based on its
		// priority. Queues are thenordered based on that value. This provides both
		// a randomness but also ensures that queues with supperior priority levels
		// are picked more often.
		type queueScore struct {
			name  string
			score int
		}

		var queuesWithScore []queueScore

		for name, priority := range mgr.weightedQueues {
			queuesWithScore = append(queuesWithScore, queueScore{
				name:  name,
				score: rand.Intn(priority),
			})
		}

		sort.Slice(queuesWithScore, func(i, j int) bool {
			return queuesWithScore[i].score > queuesWithScore[j].score
		})

		var uniqueQueues []string

		for _, q := range queuesWithScore {
			uniqueQueues = append(uniqueQueues, q.name)
		}

		var finalQueues []string

		// If some queues have concurrency limits that have been reached, we
		// remove those queues from the list to ensure no other jobs will be
		// fetched.
		mgr.activityMutex.Lock()
		for _, queue := range uniqueQueues {
			if mgr.ConcurrencyLimits[queue] == 0 ||
				mgr.activityPerQueue[queue] < mgr.ConcurrencyLimits[queue] {
				finalQueues = append(finalQueues, queue)
			}
		}
		mgr.activityMutex.Unlock()

		return finalQueues
	}

	return mgr.queues
}

func heartbeat(mgr *Manager) {
	timer := time.NewTicker(15 * time.Second)
	for {
		select {
		case <-timer.C:
			err := mgr.with(func(c *faktory.Client) error {
				data, err := c.Beat()
				if err != nil || data == "" {
					return err
				}
				var hash map[string]string
				err = json.Unmarshal([]byte(data), &hash)
				if err != nil {
					return err
				}

				if hash["state"] == "terminate" {
					handleEvent(Shutdown, mgr)
				} else if hash["state"] == "quiet" {
					handleEvent(Quiet, mgr)
				}
				return nil
			})

			switch err {
			case nil:
				mgr.Logger.Infof("Faktory heartbeat successful: %v", faktory.RandomProcessWid)
			case ErrClosed:
				mgr.Logger.Infof("Faktory client is shutting down, stopping heartbeat")
				return
			default:
				mgr.Logger.Errorf("Faktory heartbeat failed for %v: %v", faktory.RandomProcessWid, err)
			}
		}
	}
}

func handleEvent(sig eventType, mgr *Manager) {
	switch sig {
	case Shutdown:
		go func() {
			mgr.Terminate()
		}()
	case Quiet:
		go func() {
			mgr.Quiet()
		}()
	}
}

func process(mgr *Manager, idx int) {
	mgr.shutdownWaiter.Add(1)
	// delay initial fetch randomly to prevent thundering herd.
	time.Sleep(time.Duration(rand.Int31()))
	defer mgr.shutdownWaiter.Done()

	for {
		if mgr.quiet {
			return
		}

		// check for shutdown
		select {
		case <-mgr.done:
			return
		default:
		}

		// fetch job
		var job *faktory.Job
		var err error

		err = mgr.with(func(c *faktory.Client) error {
			job, err = c.Fetch(mgr.queueList()...)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			mgr.Logger.Error(err)
			time.Sleep(1 * time.Second)
			continue
		}

		// execute
		if job != nil {
			perform := mgr.jobHandlers[job.Type]
			if perform == nil {
				_ = mgr.with(func(c *faktory.Client) error {
					return c.Fail(job.Jid, fmt.Errorf("No handler for %s", job.Type), nil)
				})

			} else {
				// We increment the activity per queue at the beginning of the
				// job.
				if mgr.ConcurrencyLimits[job.Queue] > 0 {
					mgr.activityMutex.Lock()
					mgr.activityPerQueue[job.Queue]++
					mgr.activityMutex.Unlock()
				}

				h := perform
				for i := len(mgr.middleware) - 1; i >= 0; i-- {
					h = mgr.middleware[i](h)
				}

				ctx, cancel := ctxFor(mgr.context, job)
				err := h(ctx, job)

				_ = mgr.with(func(c *faktory.Client) error {
					if err != nil {
						return c.Fail(job.Jid, err, nil)
					} else {
						return c.Ack(job.Jid)
					}
				})

				cancel()

				// We decrement the activity per queue at the end of the job.
				if mgr.ConcurrencyLimits[job.Queue] > 0 {
					mgr.activityMutex.Lock()
					mgr.activityPerQueue[job.Queue]--
					mgr.activityMutex.Unlock()
				}
			}

		} else {
			// if there are no jobs, Faktory will block us on
			// the first queue for up to 2 seconds, so no need to poll or sleep. To
			// avoid hammering the server, we pause for 30 seconds if they are no
			// jobs.
			time.Sleep(time.Second * 30)
		}
	}
}

func (mgr *Manager) fireEvent(event eventType) {
	for _, fn := range mgr.eventHandlers[event] {
		fn()
	}
}

// DefaultContext embeds Go's standard context and associates it with a job ID.
type DefaultContext struct {
	context.Context

	JID  string
	Type string
}

// Jid returns the job ID for the default context
func (c *DefaultContext) Jid() string {
	return c.JID
}

// JobType returns the job type for the default context
func (c *DefaultContext) JobType() string {
	return c.Type
}

func ctxFor(ctx context.Context, job *faktory.Job) (Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(job.ReserveFor))

	return &DefaultContext{
		Context: ctx,
		JID:     job.Jid,
		Type:    job.Type,
	}, cancel
}

func (mgr *Manager) with(fn func(fky *faktory.Client) error) error {
	err := retry(3, time.Second, func() error {
		conn, err := mgr.Pool.Get()
		if err != nil {
			return err
		}

		pc := conn.(*PoolConn)
		f, ok := pc.Closeable.(*faktory.Client)
		if !ok {
			return fmt.Errorf("Connection is not a Faktory client instance: %+v", conn)
		}

		err = fn(f)
		if err != nil {
			pc.MarkUnusable()
		}

		conn.Close()

		return err
	})

	return err
}

func queueKeys(queues map[string]int) []string {
	keys := make([]string, len(queues))
	i := 0
	for k := range queues {
		keys[i] = k
		i++
	}
	// queues has to be stable so we can write tests
	sort.Strings(keys)
	return keys
}

func retry(attempts int, retryDelay time.Duration, callback func() error) (err error) {
	for i := 0; ; i++ {
		err = callback()

		if err == nil {
			return nil
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(time.Duration(retryDelay))
	}

	return err
}
