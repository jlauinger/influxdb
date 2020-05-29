package storageflux_test

import (
	"context"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/generate"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/storage/readservice"
	"go.uber.org/zap/zaptest"
)

type SetupFunc func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange)

type StorageReader struct {
	Org    influxdb.ID
	Bucket influxdb.ID
	Bounds execute.Bounds
	Close  func()
	query.StorageReader
}

func NewStorageReader(tb testing.TB, setupFn SetupFunc) *StorageReader {
	logger := zaptest.NewLogger(tb)
	rootDir, err := ioutil.TempDir("", "storage-flux-test")
	if err != nil {
		tb.Fatal(err)
	}
	close := func() { _ = os.RemoveAll(rootDir) }

	idgen := mock.NewMockIDGenerator()
	org, bucket := idgen.ID(), idgen.ID()
	sg, tr := setupFn(org, bucket)

	generator := generate.Generator{}
	if _, err := generator.Run(context.Background(), rootDir, sg); err != nil {
		tb.Fatal(err)
	}

	enginePath := filepath.Join(rootDir, "engine")
	engine := storage.NewEngine(enginePath, storage.NewConfig())
	engine.WithLogger(logger)

	if err := engine.Open(context.Background()); err != nil {
		tb.Fatal(err)
	}
	reader := storageflux.NewReader(readservice.NewStore(engine))
	return &StorageReader{
		Org:    org,
		Bucket: bucket,
		Bounds: execute.Bounds{
			Start: values.ConvertTime(tr.Start),
			Stop:  values.ConvertTime(tr.End),
		},
		Close:         close,
		StorageReader: reader,
	}
}

func (r *StorageReader) ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	wr := r.StorageReader.(query.WindowAggregateReader)
	return wr.ReadWindowAggregate(ctx, spec, alloc)
}

func TestStorageReader_ReadWindowAggregate(t *testing.T) {
	reader := NewStorageReader(t, func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange) {
		// generate a consistent output using the generator api
		// use this generator instead of the random one used by the benchmark.
		// gen.NewFloatArrayValuesSequence()
		panic("implement me")
	})

	mem := &memory.Allocator{}
	ti, err := reader.ReadWindowAggregate(context.Background(), query.ReadWindowAggregateSpec{}, mem)
	if err != nil {
		t.Fatal(err)
	}

	want := executetest.NewResult([]*executetest.Table{
		// fill in expected tables
	})
	want.Normalize()

	gotTables := []*executetest.Table{}
	if err := ti.Do(func(table flux.Table) error {
		t, err := executetest.ConvertTable(table)
		if err != nil {
			return err
		}
		gotTables = append(gotTables, t)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	got := executetest.NewResult(gotTables)
	got.Normalize()

	// compare these two

	// logger := zaptest.NewLogger(b)
	// rootDir, err := ioutil.TempDir("", "storage-reads-test")
	// if err != nil {
	// 	b.Fatal(err)
	// }
	// defer func() { _ = os.RemoveAll(rootDir) }()
	//
	// generator := generate.Generator{}
	// if _, err := generator.Run(context.Background(), rootDir, sg); err != nil {
	// 	b.Fatal(err)
	// }
	//
	// enginePath := filepath.Join(rootDir, "engine")
	// engine := storage.NewEngine(enginePath, storage.NewConfig())
	// engine.WithLogger(logger)
	//
	// if err := engine.Open(context.Background()); err != nil {
	// 	b.Fatal(err)
	// }
	// reader := storageflux.NewReader(readservice.NewStore(engine))
}

func BenchmarkReadFilter(b *testing.B) {
	setupFn := func(org, bucket influxdb.ID) (gen.SeriesGenerator, gen.TimeRange) {
		tagsSpec := &gen.TagsSpec{
			Tags: []*gen.TagValuesSpec{
				{
					TagKey: "t0",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("a-%d", 0, 5)
					},
				},
				{
					TagKey: "t1",
					Values: func() gen.CountableSequence {
						return gen.NewCounterByteSequence("b-%d", 0, 1000)
					},
				},
			},
		}
		spec := gen.Spec{
			OrgID:    org,
			BucketID: bucket,
			Measurements: []gen.MeasurementSpec{
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f0",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							r := rand.New(rand.NewSource(10))
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatRandomValuesSequence(0, 90, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							r := rand.New(rand.NewSource(11))
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatRandomValuesSequence(0, 180, r),
							)
						},
					},
				},
				{
					Name:     "m0",
					TagsSpec: tagsSpec,
					FieldValuesSpec: &gen.FieldValuesSpec{
						Name: "f1",
						TimeSequenceSpec: gen.TimeSequenceSpec{
							Count: math.MaxInt32,
							Delta: time.Minute,
						},
						DataType: models.Float,
						Values: func(spec gen.TimeSequenceSpec) gen.TimeValuesSequence {
							r := rand.New(rand.NewSource(12))
							return gen.NewTimeFloatValuesSequence(
								spec.Count,
								gen.NewTimestampSequenceFromSpec(spec),
								gen.NewFloatRandomValuesSequence(10, 10000, r),
							)
						},
					},
				},
			},
		}
		tr := gen.TimeRange{
			Start: mustParseTime("2019-11-25T00:00:00Z"),
			End:   mustParseTime("2019-11-26T00:00:00Z"),
		}
		return gen.NewSeriesGeneratorFromSpec(&spec, tr), tr
	}
	benchmarkRead(b, setupFn, func(r *StorageReader) error {
		mem := &memory.Allocator{}
		tables, err := r.ReadFilter(context.Background(), query.ReadFilterSpec{
			OrganizationID: r.Org,
			BucketID:       r.Bucket,
			Bounds:         r.Bounds,
		}, mem)
		if err != nil {
			return err
		}
		return tables.Do(func(table flux.Table) error {
			table.Done()
			return nil
		})
	})
}

func benchmarkRead(b *testing.B, setupFn SetupFunc, f func(r *StorageReader) error) {
	reader := NewStorageReader(b, setupFn)
	defer reader.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := f(reader); err != nil {
			b.Fatal(err)
		}
	}
}

func mustParseTime(s string) time.Time {
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return ts
}
