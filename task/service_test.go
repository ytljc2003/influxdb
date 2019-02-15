package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb"
	ibolt "github.com/influxdata/influxdb/bolt"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task"
	"github.com/influxdata/influxdb/task/backend"
	boltstore "github.com/influxdata/influxdb/task/backend/bolt"
	"github.com/influxdata/influxdb/task/backend/coordinator"
	"github.com/influxdata/influxdb/task/backend/executor"
	"github.com/influxdata/influxdb/task/servicetest"
	"go.uber.org/zap/zaptest"
)

func inMemFactory(t *testing.T) (*servicetest.System, context.CancelFunc) {
	st := backend.NewInMemStore()
	lrw := backend.NewInMemRunReaderWriter()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		st.Close()
	}()

	return &servicetest.System{S: st, LR: lrw, LW: lrw, Ctx: ctx, I: inmem.NewService()}, cancel
}

func boltFactory(t *testing.T) (*servicetest.System, context.CancelFunc) {
	lrw := backend.NewInMemRunReaderWriter()

	f, err := ioutil.TempFile("", "platform_adapter_test_bolt")
	if err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(f.Name(), 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	st, err := boltstore.New(db, "testbucket")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		if err := st.Close(); err != nil {
			t.Logf("error closing bolt: %v", err)
		}
		if err := os.Remove(f.Name()); err != nil {
			t.Logf("error removing bolt tempfile: %v", err)
		}
	}()

	return &servicetest.System{S: st, LR: lrw, LW: lrw, Ctx: ctx, I: inmem.NewService()}, cancel
}

func TestTaskService(t *testing.T) {
	t.Run("in-mem", func(t *testing.T) {
		t.Parallel()
		servicetest.TestTaskService(t, inMemFactory)
	})

	t.Run("bolt", func(t *testing.T) {
		t.Parallel()
		servicetest.TestTaskService(t, boltFactory)
	})
}

func TestBoltSessionTask(t *testing.T) {
	// Underlying bolt file.
	f, err := ioutil.TempFile("", "platform_adapter_test_bolt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	// Bolt client.
	logger := zaptest.NewLogger(t)
	bc := ibolt.NewClient()
	bc.Path = f.Name()
	bc.WithLogger(logger)
	if err := bc.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	// Task store for bolt.
	st, err := boltstore.New(bc.DB(), "testbucket")
	if err != nil {
		t.Fatal(err)
	}

	// Task service.
	ctx := context.Background()
	startTime := time.Now().UTC().Unix()
	lrw := backend.NewInMemRunReaderWriter()
	qs := newFakeQueryService()
	ex := executor.NewAsyncQueryServiceExecutor(logger, qs, bc, st)
	sch := backend.NewScheduler(st, ex, lrw, startTime, backend.WithLogger(logger))
	sch.Start(ctx)
	defer sch.Stop()

	coord := coordinator.New(logger, sch, st)
	pa := task.PlatformAdapter(coord, lrw, sch, bc)
	ts := task.NewValidator(pa, bc)

	// Set up user and org.
	u := &influxdb.User{Name: "u"}
	if err := bc.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: "o"}
	if err := bc.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	// Map user to org.
	if err := bc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   o.ID,
		UserID:       u.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	// Authorization... do we need it?
	authz := influxdb.Authorization{
		OrgID:       o.ID,
		UserID:      u.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := bc.CreateAuthorization(ctx, &authz); err != nil {
		t.Fatal(err)
	}

	// Source and destination buckets for use in task.
	bSrc := influxdb.Bucket{OrganizationID: o.ID, Name: "b-src"}
	if err := bc.CreateBucket(ctx, &bSrc); err != nil {
		t.Fatal(err)
	}
	bDst := influxdb.Bucket{OrganizationID: o.ID, Name: "b-dst"}
	if err := bc.CreateBucket(ctx, &bDst); err != nil {
		t.Fatal(err)
	}

	// Make a session.
	session, err := bc.CreateSession(ctx, u.Name)
	if err != nil {
		t.Fatal(err)
	}
	// Update the permissions so we can create a task.
	// I don't know how the full set of permissions on a session are calculated normally.
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.TasksResourceType, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	session.Permissions = append(session.Permissions, *p)
	p, err = influxdb.NewPermissionAtID(bDst.ID, influxdb.WriteAction, influxdb.BucketsResourceType, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	session.Permissions = append(session.Permissions, *p)
	// Save the session with updated permissions.
	if err := bc.PutSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", session)

	// Create a task, authorizing with the sesion.
	sessionCtx := icontext.SetAuthorizer(ctx, session)
	const script = `option task = {name:"x", every: 1s} from(bucket:"b-src") |> range(start:-1m) |> to(bucket:"b-dst", org:"o")`
	tsk, err := ts.CreateTask(sessionCtx, influxdb.TaskCreate{
		OrganizationID: o.ID,
		Flux:           script,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", tsk)

	// Tick ahead to trigger a run of the task just created.
	sch.Tick(time.Now().Unix() + 2)
	qs.WaitForQueryLive(t, script)

	// There should be a single run in progress.
	runs, err := lrw.ListRuns(ctx, influxdb.RunFilter{Org: &o.ID, Task: &tsk.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Logf("expected exactly 1 run, got %#v", runs)
	}
	t.Logf(runs[0].Status)
}

// Fake query service copied from task/backend/executor/executor_test.go.
type fakeQueryService struct {
	mu       sync.Mutex
	queries  map[string]*fakeQuery
	queryErr error
	// The most recent ctx received in the Query method.
	// Used to validate that the executor applied the correct authorizer.
	mostRecentCtx context.Context
}

var _ query.AsyncQueryService = (*fakeQueryService)(nil)

func makeSpec(q string) *flux.Spec {
	qs, err := flux.Compile(context.Background(), q, time.Unix(123, 0))
	if err != nil {
		panic(err)
	}
	return qs
}

func makeSpecString(q *flux.Spec) string {
	b, err := json.Marshal(q)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newFakeQueryService() *fakeQueryService {
	return &fakeQueryService{queries: make(map[string]*fakeQuery)}
}

func (s *fakeQueryService) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mostRecentCtx = ctx
	if s.queryErr != nil {
		err := s.queryErr
		s.queryErr = nil
		return nil, err
	}

	sc, ok := req.Compiler.(lang.SpecCompiler)
	if !ok {
		return nil, fmt.Errorf("fakeQueryService only supports the SpecCompiler, got %T", req.Compiler)
	}

	fq := &fakeQuery{
		wait:  make(chan struct{}),
		ready: make(chan map[string]flux.Result),
	}
	s.queries[makeSpecString(sc.Spec)] = fq

	go fq.run(ctx)

	return fq, nil
}

// SucceedQuery allows the running query matching the given script to return on its Ready channel.
func (s *fakeQueryService) SucceedQuery(script string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unblock the flux.
	spec := makeSpecString(makeSpec(script))
	close(s.queries[spec].wait)
	delete(s.queries, spec)
}

// FailQuery closes the running query's Ready channel and sets its error to the given value.
func (s *fakeQueryService) FailQuery(script string, forced error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unblock the flux.
	spec := makeSpecString(makeSpec(script))
	s.queries[spec].forcedError = forced
	close(s.queries[spec].wait)
	delete(s.queries, spec)
}

// FailNextQuery causes the next call to QueryWithCompile to return the given error.
func (s *fakeQueryService) FailNextQuery(forced error) {
	s.queryErr = forced
}

// WaitForQueryLive ensures that the query has made it into the service.
// This is particularly useful for the synchronous executor,
// because the execution starts on a separate goroutine.
func (s *fakeQueryService) WaitForQueryLive(t *testing.T, script string) {
	t.Helper()

	const attempts = 10
	spec := makeSpecString(makeSpec(script))
	for i := 0; i < attempts; i++ {
		if i != 0 {
			time.Sleep(5 * time.Millisecond)
		}

		s.mu.Lock()
		_, ok := s.queries[spec]
		s.mu.Unlock()
		if ok {
			return
		}
	}

	t.Fatalf("Did not see live query %q in time", script)
}

type fakeQuery struct {
	ready       chan map[string]flux.Result
	wait        chan struct{} // Blocks Ready from returning.
	forcedError error         // Value to return from Err() method.

	ctxErr error // Error from ctx.Done.
}

var _ flux.Query = (*fakeQuery)(nil)

func (q *fakeQuery) Spec() *flux.Spec                     { return nil }
func (q *fakeQuery) Done()                                {}
func (q *fakeQuery) Cancel()                              { close(q.ready) }
func (q *fakeQuery) Statistics() flux.Statistics          { return flux.Statistics{} }
func (q *fakeQuery) Ready() <-chan map[string]flux.Result { return q.ready }

func (q *fakeQuery) Err() error {
	if q.ctxErr != nil {
		return q.ctxErr
	}
	return q.forcedError
}

// run is intended to be run on its own goroutine.
// It blocks until q.wait is closed, then sends a fake result on the q.ready channel.
func (q *fakeQuery) run(ctx context.Context) {
	// Wait for call to set query success/fail.
	select {
	case <-ctx.Done():
		q.ctxErr = ctx.Err()
		close(q.ready)
		return
	case <-q.wait:
		// Normal case.
	}

	if q.forcedError == nil {
		res := newFakeResult()
		q.ready <- map[string]flux.Result{
			res.Name(): res,
		}
	} else {
		close(q.ready)
	}
}

// fakeResult is a dumb implementation of flux.Result that always returns the same values.
type fakeResult struct {
	name  string
	table flux.Table
}

var _ flux.Result = (*fakeResult)(nil)

func newFakeResult() *fakeResult {
	meta := []flux.ColMeta{{Label: "x", Type: flux.TInt}}
	vals := []values.Value{values.NewInt(int64(1))}
	gk := execute.NewGroupKey(meta, vals)
	a := &memory.Allocator{}
	b := execute.NewColListTableBuilder(gk, a)
	i, _ := b.AddCol(meta[0])
	b.AppendInt(i, int64(1))
	t, err := b.Table()
	if err != nil {
		panic(err)
	}
	return &fakeResult{name: "res", table: t}
}

func (r *fakeResult) Statistics() flux.Statistics {
	return flux.Statistics{}
}

func (r *fakeResult) Name() string               { return r.name }
func (r *fakeResult) Tables() flux.TableIterator { return tables{r.table} }

// tables makes a TableIterator out of a slice of Tables.
type tables []flux.Table

var _ flux.TableIterator = tables(nil)

func (ts tables) Do(f func(flux.Table) error) error {
	for _, t := range ts {
		if err := f(t); err != nil {
			return err
		}
	}
	return nil
}

func (ts tables) Statistics() flux.Statistics { return flux.Statistics{} }
