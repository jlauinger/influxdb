// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: table.gen.go.tmpl

package storageflux

import (
	"sync"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	storage "github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

//
// *********** Float ***********
//

type floatTable struct {
	table
	mu    sync.Mutex
	cur   cursors.FloatArrayCursor
	alloc *memory.Allocator
}

func newFloatTable(
	done chan struct{},
	cur cursors.FloatArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *floatTable {
	t := &floatTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *floatTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *floatTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *floatTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *floatTable) advance() bool {
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

// window table
type floatWindowTable struct {
	floatTable
	windowEvery int64
	arr         *cursors.FloatArray
	nextTS      int64
	idxInArr    int
	createEmpty bool
}

func newFloatWindowTable(
	done chan struct{},
	cur cursors.FloatArrayCursor,
	bounds execute.Bounds,
	every int64,
	createEmpty bool,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *floatWindowTable {
	t := &floatWindowTable{
		floatTable: floatTable{
			table: newTable(done, bounds, key, cols, defs, cache, alloc),
			cur:   cur,
		},
		windowEvery: every,
		createEmpty: createEmpty,
	}
	if t.createEmpty {
		start := int64(bounds.Start)
		t.nextTS = start + (every - start%every)
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *floatWindowTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

// createNextWindow will read the timestamps from the array
// cursor and construct the values for the next window.
func (t *floatWindowTable) createNextWindow() (key flux.GroupKey, start, stop *array.Int64, ok bool) {
	var stopT int64
	if t.createEmpty {
		stopT = t.nextTS
		t.nextTS += t.windowEvery
	} else {
		if !t.nextBuffer() {
			return nil, nil, nil, false
		}
		stopT = t.arr.Timestamps[t.idxInArr]
	}

	// Regain the window start time from the window end time.
	startT := stopT - t.windowEvery
	if startT < int64(t.bounds.Start) {
		startT = int64(t.bounds.Start)
	}
	if stopT > int64(t.bounds.Stop) {
		stopT = int64(t.bounds.Stop)
	}

	// If the start time is after our stop boundary,
	// we exit here when create empty is true.
	if t.createEmpty && startT >= int64(t.bounds.Stop) {
		return nil, nil, nil, false
	}
	key = groupKeyForWindow(t.key, startT, stopT)
	start = arrow.NewInt([]int64{startT}, t.alloc)
	stop = arrow.NewInt([]int64{stopT}, t.alloc)
	return key, start, stop, true
}

// nextAt will retrieve the next value that can be used with
// the given timestamp. If no values can be used with the timestamp,
// it will return the default value and false.
func (t *floatWindowTable) nextAt(ts int64) (v float64, ok bool) {
	if !t.nextBuffer() || ts > t.arr.Timestamps[t.idxInArr] {
		return
	}
	v, ok = t.arr.Values[t.idxInArr], true
	t.idxInArr++
	return v, ok
}

// nextBuffer will ensure the array cursor is filled
// and will return true if there is at least one value
// that can be read from it.
func (t *floatWindowTable) nextBuffer() bool {
	// Discard the current array cursor if we have
	// exceeded it.
	if t.arr != nil && t.idxInArr >= t.arr.Len() {
		t.arr = nil
	}

	// Retrieve the next array cursor if needed.
	if t.arr == nil {
		arr := t.cur.Next()
		if arr.Len() == 0 {
			return false
		}
		t.arr, t.idxInArr = arr, 0
	}
	return true
}

// appendValues will scan the timestamps and append values
// that match those timestamps from the buffer.
func (t *floatWindowTable) appendValues(intervals []int64, appendValue func(v float64), appendNull func()) {
	for i := 0; i < len(intervals); i++ {
		if v, ok := t.nextAt(intervals[i]); ok {
			appendValue(v)
			continue
		}
		appendNull()
	}
}

func (t *floatWindowTable) advance() bool {
	// Create the timestamps for the next window.
	key, start, stop, ok := t.createNextWindow()
	if !ok {
		return false
	}
	values := t.mergeValues(stop.Int64Values())

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(stop.Len())
	cr.key = key
	cr.cols[startColIdx] = start
	cr.cols[stopColIdx] = stop
	cr.cols[windowedValueColIdx] = values
	t.appendTags(cr)
	return true
}

// group table

type floatGroupTable struct {
	table
	mu  sync.Mutex
	gc  storage.GroupCursor
	cur cursors.FloatArrayCursor
}

func newFloatGroupTable(
	done chan struct{},
	gc storage.GroupCursor,
	cur cursors.FloatArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *floatGroupTable {
	t := &floatGroupTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *floatGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *floatGroupTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *floatGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

func (t *floatGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.FloatArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = &influxdb.Error{
				Code: influxdb.EInvalid,
				Err: &GroupCursorError{
					typ:    "float",
					cursor: cur,
				},
			}
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *floatGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** Integer ***********
//

type integerTable struct {
	table
	mu    sync.Mutex
	cur   cursors.IntegerArrayCursor
	alloc *memory.Allocator
}

func newIntegerTable(
	done chan struct{},
	cur cursors.IntegerArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *integerTable {
	t := &integerTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *integerTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *integerTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *integerTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *integerTable) advance() bool {
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

// window table
type integerWindowTable struct {
	integerTable
	windowEvery int64
	arr         *cursors.IntegerArray
	nextTS      int64
	idxInArr    int
	createEmpty bool
}

func newIntegerWindowTable(
	done chan struct{},
	cur cursors.IntegerArrayCursor,
	bounds execute.Bounds,
	every int64,
	createEmpty bool,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *integerWindowTable {
	t := &integerWindowTable{
		integerTable: integerTable{
			table: newTable(done, bounds, key, cols, defs, cache, alloc),
			cur:   cur,
		},
		windowEvery: every,
		createEmpty: createEmpty,
	}
	if t.createEmpty {
		start := int64(bounds.Start)
		t.nextTS = start + (every - start%every)
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *integerWindowTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

// createNextWindow will read the timestamps from the array
// cursor and construct the values for the next window.
func (t *integerWindowTable) createNextWindow() (key flux.GroupKey, start, stop *array.Int64, ok bool) {
	var stopT int64
	if t.createEmpty {
		stopT = t.nextTS
		t.nextTS += t.windowEvery
	} else {
		if !t.nextBuffer() {
			return nil, nil, nil, false
		}
		stopT = t.arr.Timestamps[t.idxInArr]
	}

	// Regain the window start time from the window end time.
	startT := stopT - t.windowEvery
	if startT < int64(t.bounds.Start) {
		startT = int64(t.bounds.Start)
	}
	if stopT > int64(t.bounds.Stop) {
		stopT = int64(t.bounds.Stop)
	}

	// If the start time is after our stop boundary,
	// we exit here when create empty is true.
	if t.createEmpty && startT >= int64(t.bounds.Stop) {
		return nil, nil, nil, false
	}
	key = groupKeyForWindow(t.key, startT, stopT)
	start = arrow.NewInt([]int64{startT}, t.alloc)
	stop = arrow.NewInt([]int64{stopT}, t.alloc)
	return key, start, stop, true
}

// nextAt will retrieve the next value that can be used with
// the given timestamp. If no values can be used with the timestamp,
// it will return the default value and false.
func (t *integerWindowTable) nextAt(ts int64) (v int64, ok bool) {
	if !t.nextBuffer() || ts > t.arr.Timestamps[t.idxInArr] {
		return
	}
	v, ok = t.arr.Values[t.idxInArr], true
	t.idxInArr++
	return v, ok
}

// nextBuffer will ensure the array cursor is filled
// and will return true if there is at least one value
// that can be read from it.
func (t *integerWindowTable) nextBuffer() bool {
	// Discard the current array cursor if we have
	// exceeded it.
	if t.arr != nil && t.idxInArr >= t.arr.Len() {
		t.arr = nil
	}

	// Retrieve the next array cursor if needed.
	if t.arr == nil {
		arr := t.cur.Next()
		if arr.Len() == 0 {
			return false
		}
		t.arr, t.idxInArr = arr, 0
	}
	return true
}

// appendValues will scan the timestamps and append values
// that match those timestamps from the buffer.
func (t *integerWindowTable) appendValues(intervals []int64, appendValue func(v int64), appendNull func()) {
	for i := 0; i < len(intervals); i++ {
		if v, ok := t.nextAt(intervals[i]); ok {
			appendValue(v)
			continue
		}
		appendNull()
	}
}

func (t *integerWindowTable) advance() bool {
	// Create the timestamps for the next window.
	key, start, stop, ok := t.createNextWindow()
	if !ok {
		return false
	}
	values := t.mergeValues(stop.Int64Values())

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(stop.Len())
	cr.key = key
	cr.cols[startColIdx] = start
	cr.cols[stopColIdx] = stop
	cr.cols[windowedValueColIdx] = values
	t.appendTags(cr)
	return true
}

// group table

type integerGroupTable struct {
	table
	mu  sync.Mutex
	gc  storage.GroupCursor
	cur cursors.IntegerArrayCursor
}

func newIntegerGroupTable(
	done chan struct{},
	gc storage.GroupCursor,
	cur cursors.IntegerArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *integerGroupTable {
	t := &integerGroupTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *integerGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *integerGroupTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *integerGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

func (t *integerGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.IntegerArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = &influxdb.Error{
				Code: influxdb.EInvalid,
				Err: &GroupCursorError{
					typ:    "integer",
					cursor: cur,
				},
			}
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *integerGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** Unsigned ***********
//

type unsignedTable struct {
	table
	mu    sync.Mutex
	cur   cursors.UnsignedArrayCursor
	alloc *memory.Allocator
}

func newUnsignedTable(
	done chan struct{},
	cur cursors.UnsignedArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *unsignedTable {
	t := &unsignedTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *unsignedTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *unsignedTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *unsignedTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *unsignedTable) advance() bool {
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

// window table
type unsignedWindowTable struct {
	unsignedTable
	windowEvery int64
	arr         *cursors.UnsignedArray
	nextTS      int64
	idxInArr    int
	createEmpty bool
}

func newUnsignedWindowTable(
	done chan struct{},
	cur cursors.UnsignedArrayCursor,
	bounds execute.Bounds,
	every int64,
	createEmpty bool,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *unsignedWindowTable {
	t := &unsignedWindowTable{
		unsignedTable: unsignedTable{
			table: newTable(done, bounds, key, cols, defs, cache, alloc),
			cur:   cur,
		},
		windowEvery: every,
		createEmpty: createEmpty,
	}
	if t.createEmpty {
		start := int64(bounds.Start)
		t.nextTS = start + (every - start%every)
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *unsignedWindowTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

// createNextWindow will read the timestamps from the array
// cursor and construct the values for the next window.
func (t *unsignedWindowTable) createNextWindow() (key flux.GroupKey, start, stop *array.Int64, ok bool) {
	var stopT int64
	if t.createEmpty {
		stopT = t.nextTS
		t.nextTS += t.windowEvery
	} else {
		if !t.nextBuffer() {
			return nil, nil, nil, false
		}
		stopT = t.arr.Timestamps[t.idxInArr]
	}

	// Regain the window start time from the window end time.
	startT := stopT - t.windowEvery
	if startT < int64(t.bounds.Start) {
		startT = int64(t.bounds.Start)
	}
	if stopT > int64(t.bounds.Stop) {
		stopT = int64(t.bounds.Stop)
	}

	// If the start time is after our stop boundary,
	// we exit here when create empty is true.
	if t.createEmpty && startT >= int64(t.bounds.Stop) {
		return nil, nil, nil, false
	}
	key = groupKeyForWindow(t.key, startT, stopT)
	start = arrow.NewInt([]int64{startT}, t.alloc)
	stop = arrow.NewInt([]int64{stopT}, t.alloc)
	return key, start, stop, true
}

// nextAt will retrieve the next value that can be used with
// the given timestamp. If no values can be used with the timestamp,
// it will return the default value and false.
func (t *unsignedWindowTable) nextAt(ts int64) (v uint64, ok bool) {
	if !t.nextBuffer() || ts > t.arr.Timestamps[t.idxInArr] {
		return
	}
	v, ok = t.arr.Values[t.idxInArr], true
	t.idxInArr++
	return v, ok
}

// nextBuffer will ensure the array cursor is filled
// and will return true if there is at least one value
// that can be read from it.
func (t *unsignedWindowTable) nextBuffer() bool {
	// Discard the current array cursor if we have
	// exceeded it.
	if t.arr != nil && t.idxInArr >= t.arr.Len() {
		t.arr = nil
	}

	// Retrieve the next array cursor if needed.
	if t.arr == nil {
		arr := t.cur.Next()
		if arr.Len() == 0 {
			return false
		}
		t.arr, t.idxInArr = arr, 0
	}
	return true
}

// appendValues will scan the timestamps and append values
// that match those timestamps from the buffer.
func (t *unsignedWindowTable) appendValues(intervals []int64, appendValue func(v uint64), appendNull func()) {
	for i := 0; i < len(intervals); i++ {
		if v, ok := t.nextAt(intervals[i]); ok {
			appendValue(v)
			continue
		}
		appendNull()
	}
}

func (t *unsignedWindowTable) advance() bool {
	// Create the timestamps for the next window.
	key, start, stop, ok := t.createNextWindow()
	if !ok {
		return false
	}
	values := t.mergeValues(stop.Int64Values())

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(stop.Len())
	cr.key = key
	cr.cols[startColIdx] = start
	cr.cols[stopColIdx] = stop
	cr.cols[windowedValueColIdx] = values
	t.appendTags(cr)
	return true
}

// group table

type unsignedGroupTable struct {
	table
	mu  sync.Mutex
	gc  storage.GroupCursor
	cur cursors.UnsignedArrayCursor
}

func newUnsignedGroupTable(
	done chan struct{},
	gc storage.GroupCursor,
	cur cursors.UnsignedArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *unsignedGroupTable {
	t := &unsignedGroupTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *unsignedGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *unsignedGroupTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *unsignedGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

func (t *unsignedGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.UnsignedArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = &influxdb.Error{
				Code: influxdb.EInvalid,
				Err: &GroupCursorError{
					typ:    "unsigned",
					cursor: cur,
				},
			}
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *unsignedGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** String ***********
//

type stringTable struct {
	table
	mu    sync.Mutex
	cur   cursors.StringArrayCursor
	alloc *memory.Allocator
}

func newStringTable(
	done chan struct{},
	cur cursors.StringArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *stringTable {
	t := &stringTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *stringTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *stringTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *stringTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *stringTable) advance() bool {
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

// window table
type stringWindowTable struct {
	stringTable
	windowEvery int64
	arr         *cursors.StringArray
	nextTS      int64
	idxInArr    int
	createEmpty bool
}

func newStringWindowTable(
	done chan struct{},
	cur cursors.StringArrayCursor,
	bounds execute.Bounds,
	every int64,
	createEmpty bool,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *stringWindowTable {
	t := &stringWindowTable{
		stringTable: stringTable{
			table: newTable(done, bounds, key, cols, defs, cache, alloc),
			cur:   cur,
		},
		windowEvery: every,
		createEmpty: createEmpty,
	}
	if t.createEmpty {
		start := int64(bounds.Start)
		t.nextTS = start + (every - start%every)
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *stringWindowTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

// createNextWindow will read the timestamps from the array
// cursor and construct the values for the next window.
func (t *stringWindowTable) createNextWindow() (key flux.GroupKey, start, stop *array.Int64, ok bool) {
	var stopT int64
	if t.createEmpty {
		stopT = t.nextTS
		t.nextTS += t.windowEvery
	} else {
		if !t.nextBuffer() {
			return nil, nil, nil, false
		}
		stopT = t.arr.Timestamps[t.idxInArr]
	}

	// Regain the window start time from the window end time.
	startT := stopT - t.windowEvery
	if startT < int64(t.bounds.Start) {
		startT = int64(t.bounds.Start)
	}
	if stopT > int64(t.bounds.Stop) {
		stopT = int64(t.bounds.Stop)
	}

	// If the start time is after our stop boundary,
	// we exit here when create empty is true.
	if t.createEmpty && startT >= int64(t.bounds.Stop) {
		return nil, nil, nil, false
	}
	key = groupKeyForWindow(t.key, startT, stopT)
	start = arrow.NewInt([]int64{startT}, t.alloc)
	stop = arrow.NewInt([]int64{stopT}, t.alloc)
	return key, start, stop, true
}

// nextAt will retrieve the next value that can be used with
// the given timestamp. If no values can be used with the timestamp,
// it will return the default value and false.
func (t *stringWindowTable) nextAt(ts int64) (v string, ok bool) {
	if !t.nextBuffer() || ts > t.arr.Timestamps[t.idxInArr] {
		return
	}
	v, ok = t.arr.Values[t.idxInArr], true
	t.idxInArr++
	return v, ok
}

// nextBuffer will ensure the array cursor is filled
// and will return true if there is at least one value
// that can be read from it.
func (t *stringWindowTable) nextBuffer() bool {
	// Discard the current array cursor if we have
	// exceeded it.
	if t.arr != nil && t.idxInArr >= t.arr.Len() {
		t.arr = nil
	}

	// Retrieve the next array cursor if needed.
	if t.arr == nil {
		arr := t.cur.Next()
		if arr.Len() == 0 {
			return false
		}
		t.arr, t.idxInArr = arr, 0
	}
	return true
}

// appendValues will scan the timestamps and append values
// that match those timestamps from the buffer.
func (t *stringWindowTable) appendValues(intervals []int64, appendValue func(v string), appendNull func()) {
	for i := 0; i < len(intervals); i++ {
		if v, ok := t.nextAt(intervals[i]); ok {
			appendValue(v)
			continue
		}
		appendNull()
	}
}

func (t *stringWindowTable) advance() bool {
	// Create the timestamps for the next window.
	key, start, stop, ok := t.createNextWindow()
	if !ok {
		return false
	}
	values := t.mergeValues(stop.Int64Values())

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(stop.Len())
	cr.key = key
	cr.cols[startColIdx] = start
	cr.cols[stopColIdx] = stop
	cr.cols[windowedValueColIdx] = values
	t.appendTags(cr)
	return true
}

// group table

type stringGroupTable struct {
	table
	mu  sync.Mutex
	gc  storage.GroupCursor
	cur cursors.StringArrayCursor
}

func newStringGroupTable(
	done chan struct{},
	gc storage.GroupCursor,
	cur cursors.StringArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *stringGroupTable {
	t := &stringGroupTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *stringGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *stringGroupTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *stringGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

func (t *stringGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.StringArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = &influxdb.Error{
				Code: influxdb.EInvalid,
				Err: &GroupCursorError{
					typ:    "string",
					cursor: cur,
				},
			}
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *stringGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** Boolean ***********
//

type booleanTable struct {
	table
	mu    sync.Mutex
	cur   cursors.BooleanArrayCursor
	alloc *memory.Allocator
}

func newBooleanTable(
	done chan struct{},
	cur cursors.BooleanArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *booleanTable {
	t := &booleanTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *booleanTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *booleanTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *booleanTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *booleanTable) advance() bool {
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

// window table
type booleanWindowTable struct {
	booleanTable
	windowEvery int64
	arr         *cursors.BooleanArray
	nextTS      int64
	idxInArr    int
	createEmpty bool
}

func newBooleanWindowTable(
	done chan struct{},
	cur cursors.BooleanArrayCursor,
	bounds execute.Bounds,
	every int64,
	createEmpty bool,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *booleanWindowTable {
	t := &booleanWindowTable{
		booleanTable: booleanTable{
			table: newTable(done, bounds, key, cols, defs, cache, alloc),
			cur:   cur,
		},
		windowEvery: every,
		createEmpty: createEmpty,
	}
	if t.createEmpty {
		start := int64(bounds.Start)
		t.nextTS = start + (every - start%every)
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *booleanWindowTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

// createNextWindow will read the timestamps from the array
// cursor and construct the values for the next window.
func (t *booleanWindowTable) createNextWindow() (key flux.GroupKey, start, stop *array.Int64, ok bool) {
	var stopT int64
	if t.createEmpty {
		stopT = t.nextTS
		t.nextTS += t.windowEvery
	} else {
		if !t.nextBuffer() {
			return nil, nil, nil, false
		}
		stopT = t.arr.Timestamps[t.idxInArr]
	}

	// Regain the window start time from the window end time.
	startT := stopT - t.windowEvery
	if startT < int64(t.bounds.Start) {
		startT = int64(t.bounds.Start)
	}
	if stopT > int64(t.bounds.Stop) {
		stopT = int64(t.bounds.Stop)
	}

	// If the start time is after our stop boundary,
	// we exit here when create empty is true.
	if t.createEmpty && startT >= int64(t.bounds.Stop) {
		return nil, nil, nil, false
	}
	key = groupKeyForWindow(t.key, startT, stopT)
	start = arrow.NewInt([]int64{startT}, t.alloc)
	stop = arrow.NewInt([]int64{stopT}, t.alloc)
	return key, start, stop, true
}

// nextAt will retrieve the next value that can be used with
// the given timestamp. If no values can be used with the timestamp,
// it will return the default value and false.
func (t *booleanWindowTable) nextAt(ts int64) (v bool, ok bool) {
	if !t.nextBuffer() || ts > t.arr.Timestamps[t.idxInArr] {
		return
	}
	v, ok = t.arr.Values[t.idxInArr], true
	t.idxInArr++
	return v, ok
}

// nextBuffer will ensure the array cursor is filled
// and will return true if there is at least one value
// that can be read from it.
func (t *booleanWindowTable) nextBuffer() bool {
	// Discard the current array cursor if we have
	// exceeded it.
	if t.arr != nil && t.idxInArr >= t.arr.Len() {
		t.arr = nil
	}

	// Retrieve the next array cursor if needed.
	if t.arr == nil {
		arr := t.cur.Next()
		if arr.Len() == 0 {
			return false
		}
		t.arr, t.idxInArr = arr, 0
	}
	return true
}

// appendValues will scan the timestamps and append values
// that match those timestamps from the buffer.
func (t *booleanWindowTable) appendValues(intervals []int64, appendValue func(v bool), appendNull func()) {
	for i := 0; i < len(intervals); i++ {
		if v, ok := t.nextAt(intervals[i]); ok {
			appendValue(v)
			continue
		}
		appendNull()
	}
}

func (t *booleanWindowTable) advance() bool {
	// Create the timestamps for the next window.
	key, start, stop, ok := t.createNextWindow()
	if !ok {
		return false
	}
	values := t.mergeValues(stop.Int64Values())

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(stop.Len())
	cr.key = key
	cr.cols[startColIdx] = start
	cr.cols[stopColIdx] = stop
	cr.cols[windowedValueColIdx] = values
	t.appendTags(cr)
	return true
}

// group table

type booleanGroupTable struct {
	table
	mu  sync.Mutex
	gc  storage.GroupCursor
	cur cursors.BooleanArrayCursor
}

func newBooleanGroupTable(
	done chan struct{},
	gc storage.GroupCursor,
	cur cursors.BooleanArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) *booleanGroupTable {
	t := &booleanGroupTable{
		table: newTable(done, bounds, key, cols, defs, cache, alloc),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *booleanGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *booleanGroupTable) Do(f func(flux.ColReader) error) error {
	return t.do(f, t.advance)
}

func (t *booleanGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	l := a.Len()
	if l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	// Retrieve the buffer for the data to avoid allocating
	// additional slices. If the buffer is still being used
	// because the references were retained, then we will
	// allocate a new buffer.
	cr := t.allocateBuffer(l)
	cr.cols[timeColIdx] = arrow.NewInt(a.Timestamps, t.alloc)
	cr.cols[valueColIdx] = t.toArrowBuffer(a.Values)
	t.appendTags(cr)
	t.appendBounds(cr)
	return true
}

func (t *booleanGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.BooleanArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = &influxdb.Error{
				Code: influxdb.EInvalid,
				Err: &GroupCursorError{
					typ:    "boolean",
					cursor: cur,
				},
			}
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *booleanGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}
