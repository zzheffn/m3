// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package index

import (
	goctx "context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/segments"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	m3ninxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

var (
	errUnableToWriteBlockClosed     = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed     = errors.New("unable to write, index block is sealed")
	errUnableToQueryBlockClosed     = errors.New("unable to query, index block is closed")
	errUnableToBootstrapBlockClosed = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed      = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed           = errors.New("unable to close, block already closed")

	errUnableToSealBlockIllegalStateFmtString  = "unable to seal, index block state: %v"
	errUnableToWriteBlockUnknownStateFmtString = "unable to write, unknown index block state: %v"
)

type blockState byte

const (
	blockStateClosed blockState = iota
	blockStateOpen
	blockStateSealed
)

type doneFn func()

type newExecutorFn func() (search.Executor, doneFn, error)

type block struct {
	sync.RWMutex
	state               blockState
	shardRangesSegments []*blockShardRangesSegments
	activeSegments      []*activeSegment

	// the following are used to help compactions
	numActiveCompactions *atomic.Int64
	compactOpts          CompactionOptions
	rotateCh             chan struct{}
	closeCtx             goctx.Context
	closeCtxCancelFn     goctx.CancelFunc

	nowFn         clock.NowFn
	metrics       blockMetrics
	segmentID     atomic.Int64
	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	blockSize     time.Duration
	opts          Options
	nsMD          namespace.Metadata
}

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	startTime time.Time,
	md namespace.Metadata,
	opts Options,
) (Block, error) {
	var (
		blockSize = md.Options().IndexOptions().BlockSize()
	)

	closeCtx, closeFn := goctx.WithCancel(goctx.Background())
	b := &block{
		state: blockStateOpen,

		numActiveCompactions: atomic.NewInt64(0),
		compactOpts:          opts.CompactionOptions(),
		rotateCh:             make(chan struct{}, 1),
		closeCtx:             closeCtx,
		closeCtxCancelFn:     closeFn,

		metrics:   newBlockMetrics(opts.InstrumentOptions()),
		nowFn:     opts.ClockOptions().NowFn(),
		startTime: startTime,
		endTime:   startTime.Add(blockSize),
		blockSize: blockSize,
		opts:      opts,
		nsMD:      md,
	}
	b.newExecutorFn = b.executorWithRLock
	b.addActiveSegmentWithLock()
	go b.monitorRotations()
	return b, nil
}

func (b *block) StartTime() time.Time {
	return b.startTime
}

func (b *block) EndTime() time.Time {
	return b.endTime
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.Lock()
	defer b.Unlock()

	if b.state != blockStateOpen {
		err := b.writeBatchErrorInvalidState(b.state)
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError: int64(inserts.Len()),
		}, err
	}

	// NB: we're guaranteed the block has a mutable activeSegment because
	// of the state check above; the if check below is additional paranoia.
	mutableActiveSeg := b.mutableActiveSegmentWithRLock()
	mutableSeg := mutableActiveSeg.mutableSegment
	if mutableSeg == nil { // should never happen
		err := b.openBlockHasNilActiveSegmentInvariantErrorWithRLock()
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError: int64(inserts.Len()),
		}, err
	}

	defer func() {
		if mutableActiveSeg.MutableAndCompactable(b.compactOpts.PlannerOptions()) {
			mutableActiveSeg.writable = false
			mutableSeg.Seal()
			b.addActiveSegmentWithLock()
			b.triggerRotations()
		}
	}()

	earliestWrite, ok := inserts.PendingEntries().EarliestEnqueuedAt()
	if ok {
		mutableActiveSeg.UpdateEarliestWrite(earliestWrite)
	}

	err := mutableSeg.InsertBatch(m3ninxindex.Batch{
		Docs:                inserts.PendingDocs(),
		AllowPartialUpdates: true,
	})
	if err == nil {
		inserts.MarkUnmarkedEntriesSuccess()
		return WriteBatchResult{
			NumSuccess: int64(inserts.Len()),
		}, nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok { // should never happen
		err := b.unknownWriteBatchInvariantError(err)
		// NB: marking all the inserts as failure, cause we don't know which ones failed
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{NumError: int64(inserts.Len())}, err
	}

	numErr := len(partialErr.Errs())
	for _, err := range partialErr.Errs() {
		// Avoid marking these as success
		inserts.MarkUnmarkedEntryError(err.Err, err.Idx)
	}

	// mark all non-error inserts success, so we don't repeatedly index them
	inserts.MarkUnmarkedEntriesSuccess()
	return WriteBatchResult{
		NumSuccess: int64(inserts.Len() - numErr),
		NumError:   int64(numErr),
	}, partialErr
}

func (b *block) executorWithRLock() (search.Executor, doneFn, error) {
	expectedReaders := len(b.activeSegments)
	for _, group := range b.shardRangesSegments {
		expectedReaders += len(group.segments)
	}

	var (
		activeSegs    = make([]*activeSegment, 0, expectedReaders)
		nonActiveSegs = make([]*blockShardRangesSegments, 0, expectedReaders)
		readers       = make([]m3ninxindex.Reader, 0, expectedReaders)
		success       = false
	)

	done := func() {
		for _, seg := range activeSegs {
			seg.ReaderDone()
		}
		for _, group := range nonActiveSegs {
			group.readsWg.Done()
		}
	}

	// cleanup in case any of the readers below fail.
	defer func() {
		if !success {
			for _, reader := range readers {
				reader.Close()
			}
		}
	}()

	// start with the segment that's being actively written to (if we have one)
	for _, seg := range b.activeSegments {
		if seg.segmentType != segments.FSTType {
			continue
		}
		reader, err := seg.Reader()
		if err != nil {
			return nil, nil, err
		}
		readers = append(readers, reader)
		activeSegs = append(activeSegs, seg)
	}

	// loop over the segments associated to shard time ranges
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			reader, err := seg.Reader()
			if err != nil {
				return nil, nil, err
			}
			group.readsWg.Add(1)
			readers = append(readers, reader)
			nonActiveSegs = append(nonActiveSegs, group)
		}
	}

	success = true
	return executor.NewExecutor(readers), done, nil
}

func (b *block) Query(
	query Query,
	opts QueryOptions,
	results Results,
) (bool, error) {
	b.RLock()
	if b.state == blockStateClosed {
		b.RUnlock()
		return false, errUnableToQueryBlockClosed
	}

	exec, done, err := b.newExecutorFn()
	b.RUnlock()
	if err != nil {
		return false, err
	}
	defer done()

	// FOLLOWUP(prateek): push down QueryOptions to restrict results
	// TODO(jeromefroe): Use the idx query directly once we implement an index in m3ninx
	// and don't need to use the segments anymore.
	iter, err := exec.Execute(query.Query.SearchQuery())
	if err != nil {
		exec.Close()
		return false, err
	}

	var (
		size       = results.Size()
		brokeEarly = false
	)
	execCloser := safeCloser{closable: exec}
	iterCloser := safeCloser{closable: iter}

	defer func() {
		iterCloser.Close()
		execCloser.Close()
	}()

	for iter.Next() {
		if opts.Limit > 0 && size >= opts.Limit {
			brokeEarly = true
			break
		}
		d := iter.Current()
		_, size, err = results.Add(d)
		if err != nil {
			return false, err
		}
	}

	if err := iter.Err(); err != nil {
		return false, err
	}

	if err := iterCloser.Close(); err != nil {
		return false, err
	}

	if err := execCloser.Close(); err != nil {
		return false, err
	}

	exhaustive := !brokeEarly
	return exhaustive, nil
}

func (b *block) AddResults(
	results result.IndexBlock,
) error {
	b.Lock()
	defer b.Unlock()

	// NB(prateek): we have to allow bootstrap to succeed even if we're Sealed because
	// of topology changes. i.e. if the current m3db process is assigned new shards,
	// we need to include their data in the index.

	// i.e. the only state we do not accept bootstrapped data is if we are closed.
	if b.state == blockStateClosed {
		return errUnableToBootstrapBlockClosed
	}

	// First check fulfilled is correct
	min, max := results.Fulfilled().MinMax()
	if min.Before(b.startTime) || max.After(b.endTime) {
		blockRange := xtime.Range{Start: b.startTime, End: b.endTime}
		return fmt.Errorf("fulfilled range %s is outside of index block range: %s",
			results.Fulfilled().SummaryString(), blockRange.String())
	}

	// NB: need to check if the current block has been marked 'Sealed' and if so,
	// mark all incoming mutable segments the same.
	isSealed := b.IsSealedWithRLock()

	var multiErr xerrors.MultiError
	for _, seg := range results.Segments() {
		if x, ok := seg.(segment.MutableSegment); ok {
			if isSealed {
				_, err := x.Seal()
				if err != nil {
					// if this happens it means a Mutable segment was marked sealed
					// in the bootstrappers, this should never happen.
					err := b.bootstrappingSealedMutableSegmentInvariant(err)
					multiErr = multiErr.Add(err)
				}
			}
		}
	}

	entry := &blockShardRangesSegments{
		shardTimeRanges: results.Fulfilled(),
		segments:        results.Segments(),
	}

	// First see if this block can cover all our current blocks covering shard
	// time ranges
	currFulfilled := make(result.ShardTimeRanges)
	for _, existing := range b.shardRangesSegments {
		currFulfilled.AddRanges(existing.shardTimeRanges)
	}

	unfulfilledBySegments := currFulfilled.Copy()
	unfulfilledBySegments.Subtract(results.Fulfilled())
	if !unfulfilledBySegments.IsEmpty() {
		// This is the case where it cannot wholly replace the current set of blocks
		// so simply append the segments in this case
		b.shardRangesSegments = append(b.shardRangesSegments, entry)
		return multiErr.FinalError()
	}

	// This is the case where the new segments can wholly replace the
	// current set of blocks since unfullfilled by the new segments is zero
	for i, group := range b.shardRangesSegments {
		group.Close()
		b.shardRangesSegments[i] = nil
	}
	b.shardRangesSegments = append(b.shardRangesSegments[:0], entry)

	return multiErr.FinalError()
}

func (b *block) Tick(c context.Cancellable, tickStart time.Time) (BlockTickResult, error) {
	b.RLock()
	defer b.RUnlock()
	result := BlockTickResult{}
	if b.state == blockStateClosed {
		return result, errUnableToTickBlockClosed
	}

	// active segments
	for _, seg := range b.activeSegments {
		result.NumSegments++
		result.NumDocs += seg.Size()
	}

	// all other segments (bootstrapped/etc)
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			result.NumSegments++
			result.NumDocs += seg.Size()
		}
	}

	return result, nil
}

func (b *block) Seal() error {
	b.Lock()
	defer b.Unlock()

	// ensure we only Seal if we're marked Open
	if b.state != blockStateOpen {
		return fmt.Errorf(errUnableToSealBlockIllegalStateFmtString, b.state)
	}
	b.state = blockStateSealed

	var multiErr xerrors.MultiError
	// TODO(prateek): stop all on-going compactions, and force-compact all the segments below into a single segment
	// seal active mutable segments.
	for _, seg := range b.activeSegments {
		if seg.segmentType == segments.MutableType {
			_, err := seg.mutableSegment.Seal()
			multiErr = multiErr.Add(err)
		}
	}

	// TODO(prateek): ensure that all segments from flush/bootstrap are FST already and delete this code
	// loop over any added mutable segments and seal them too.
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			if unsealed, ok := seg.(segment.MutableSegment); ok {
				_, err := unsealed.Seal()
				multiErr = multiErr.Add(err)
			}
		}
	}

	return multiErr.FinalError()
}

func (b *block) IsSealedWithRLock() bool {
	return b.state == blockStateSealed
}

func (b *block) IsSealed() bool {
	b.RLock()
	defer b.RUnlock()
	return b.IsSealedWithRLock()
}

func (b *block) NeedsMutableSegmentsEvicted() bool {
	b.RLock()
	defer b.RUnlock()
	anyMutableSegmentNeedsEviction := false

	// loop thru active segments and see if they require to be flushed
	for _, seg := range b.activeSegments {
		anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || seg.Size() > 0
	}

	// can early terminate if we already know we need to flush.
	if anyMutableSegmentNeedsEviction {
		return true
	}

	// otherwise we check all the boostrapped segments and to see if any of them need a flush
	for _, shardRangeSegments := range b.shardRangesSegments {
		for _, seg := range shardRangeSegments.segments {
			if mutableSeg, ok := seg.(segment.MutableSegment); ok {
				anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || mutableSeg.Size() > 0
			}
		}
	}

	return anyMutableSegmentNeedsEviction
}

func (b *block) EvictActiveSegments() (EvictActiveSegmentResults, error) {
	var results EvictActiveSegmentResults
	b.Lock()
	defer b.Unlock()
	if b.state != blockStateSealed {
		return results, fmt.Errorf("unable to evict mutable segments, block must be sealed, found: %v", b.state)
	}
	var multiErr xerrors.MultiError

	// close active segments
	for _, seg := range b.activeSegments {
		results.NumActiveSegments++
		results.NumDocs += seg.Size()
		multiErr = multiErr.Add(seg.Seal())
		multiErr = multiErr.Add(seg.Close())
	}
	// clear any references to active segments
	b.activeSegments = nil

	return results, multiErr.FinalError()
}

func (b *block) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errBlockAlreadyClosed
	}
	b.state = blockStateClosed

	var multiErr xerrors.MultiError

	// cancel any rotations that might be happening.
	b.closeCtxCancelFn()

	// close any active segments.
	for _, seg := range b.activeSegments {
		multiErr = multiErr.Add(seg.Seal())
		multiErr = multiErr.Add(seg.Close())
	}
	b.activeSegments = nil

	// close any other added segments too.
	for _, group := range b.shardRangesSegments {
		group.Close()
	}
	b.shardRangesSegments = nil

	return multiErr.FinalError()
}

func (b *block) triggerRotations() {
	select {
	case b.rotateCh <- struct{}{}: // all good, we enqueued
	default: // i.e. there's already a rotation enqueued, so we're good.
	}
}

// monitorRotations monitors rotateCh and triggers activeSegment rotations when signaled.
func (b *block) monitorRotations() {
	for {
		select {
		case <-b.closeCtx.Done():
			return
		case <-b.rotateCh:
			b.coordinateCompactions()
		}
	}
}

func (b *block) coordinateCompactions() {
	// find group of active segments which need to be rotated
	b.Lock()
	candidates := b.newCompactionCandidatesWithRLock()
	compactionPlan, err := compaction.NewPlan(candidates, b.compactOpts.PlannerOptions())
	if err != nil {
		b.opts.InstrumentOptions().Logger().Errorf("unable to create compaction plan, err = %v", err)
		b.Unlock()
		return
	}
	// can early terminate if we don't need to do any work.
	if len(compactionPlan.Tasks) == 0 {
		b.Unlock()
		return
	}
	// mark all segments about to be compacted as such to avoid double-compacting
	b.setCompactingStatusWithLock(true, compactionPlan.Tasks)
	b.Unlock()

	// at long last, actually compact the segments
	workers := b.compactOpts.WorkerPool()
	for idx := range compactionPlan.Tasks {
		task := compactionPlan.Tasks[idx]
		workers.Go(func() { b.compactAndSwapSegments(task) })
	}
}

func (b *block) setCompactingStatusWithLock(compacting bool, tasks []compaction.Task) {
	// mark segments w/ appropriate compacting status in activeSegments
	for _, seg := range b.activeSegments {
		candidateSegment := seg.Segment()
		for _, task := range tasks {
			for _, taskSeg := range task.Segments {
				if candidateSegment == taskSeg.Segment {
					seg.compacting = compacting
				}
			}
		}
	}
}

func (b *block) compactAndSwapSegments(task compaction.Task) {
	b.numActiveCompactions.Inc()
	defer func() {
		b.numActiveCompactions.Dec()
	}()

	start := b.nowFn()
	// create slice of segments to compact
	maxCompactionNumber := int64(0)
	segmentsToCompact := make([]segment.Segment, 0, len(task.Segments))
	for _, s := range task.Segments {
		segmentsToCompact = append(segmentsToCompact, s.Segment)
		if s.CompactionNumber > maxCompactionNumber {
			maxCompactionNumber = s.CompactionNumber
		}
	}

	// TODO(prateek): we only log in failure cases right now, this leaves the segments we failed
	// to compact as "compacting" and would never be compacted again. Change this to do the following:
	// (1) Retry failed compactions
	// (2) Emit metrics and logs for cause of failed compactions

	// merge segments into single mutable segment
	postingsOffset := postings.ID(b.segmentID.Inc())
	mergedMutableSegment := mem.NewSegment(postingsOffset, b.opts.MemSegmentOptions())
	if err := mem.Merge(mergedMutableSegment, segmentsToCompact...); err != nil {
		b.opts.InstrumentOptions().Logger().Errorf("unable to merge simple segments, err = %v", err)
		return
	}

	if _, err := mergedMutableSegment.Seal(); err != nil {
		b.opts.InstrumentOptions().Logger().Errorf("unable to seal merged segment, err = %v", err)
		return
	}

	fstSegment, err := m3ninxpersist.TransformAndMmap(mergedMutableSegment, b.opts.FSTSegmentOptions())
	if err != nil {
		b.opts.InstrumentOptions().Logger().Errorf("unable to fst-ify merged segment, err = %v", err)
		return
	}

	compactionTime := b.nowFn().Sub(start)
	b.metrics.CompactionLatency.Record(compactionTime)
	mergedMutableSegment.Close() // can skip error checking here as we've got the equivalent FST
	newActiveSegment := newFSTActiveSegment(b.nowFn(), fstSegment, int(1+maxCompactionNumber))

	// swap the successfully converted activeSegments with the newly created FST
	b.Lock()
	mutableSegmentTimes := make([]time.Time, 0, 10)
	compactedActiveSegments := make([]*activeSegment, 0, len(segmentsToCompact))
	newActiveSegments := make([]*activeSegment, 0, len(b.activeSegments))
	for _, activeSeg := range b.activeSegments {
		// skip all the activeSegments which have been converted above
		isCompactedSegment := false
		for _, seg := range segmentsToCompact {
			if activeSeg.Segment() == seg {
				isCompactedSegment = true
				break
			}
		}
		// add all other activeSegments
		if !isCompactedSegment {
			newActiveSegments = append(newActiveSegments, activeSeg)
			continue
		}

		// i.e. this is a compacted segment
		compactedActiveSegments = append(compactedActiveSegments, activeSeg)

		// track earliest enqueued time for mutable segments to report e2e latency
		if activeSeg.segmentType == segments.MutableType {
			mutableSegmentTimes = append(mutableSegmentTimes, activeSeg.earliestWrite)
		}
	}
	// finally, add the newly created activeSegment and update active segments
	newActiveSegments = append(newActiveSegments, newActiveSegment)
	b.activeSegments = newActiveSegments
	b.Unlock()

	// report e2e latency for any mutable segments that are finally available now
	newSegmentAvailableTime := b.nowFn()
	for _, t := range mutableSegmentTimes {
		b.metrics.InsertEndToEndLatency.Record(newSegmentAvailableTime.Sub(t))
	}

	// release resources from all compacted segments
	for _, seg := range compactedActiveSegments {
		// can skip error checking here as we've got the equivalent compacted segment
		seg.Seal()
		seg.Close()
	}
}

func (b *block) newCompactionCandidatesWithRLock() []compaction.Segment {
	now := b.nowFn()
	candidates := make([]compaction.Segment, 0, len(b.activeSegments))
	for _, seg := range b.activeSegments {
		if seg.compacting || seg.writable {
			continue
		}
		candidates = append(candidates, compaction.Segment{
			CompactionNumber: int64(seg.compactionNumber),
			Size:             seg.Size(),
			Type:             seg.segmentType,
			Age:              now.Sub(seg.creationTime),
			Segment:          seg.Segment(),
		})
	}
	return candidates
}

func (b *block) addActiveSegmentWithLock() {
	postingsOffset := postings.ID(b.segmentID.Inc())
	mutableSeg := mem.NewSegment(postingsOffset, b.opts.MemSegmentOptions())

	// move the new mutable segment to the front to make it easier for writes
	// to find a mutable segment.
	b.activeSegments = append([]*activeSegment{
		newMutableActiveSegment(b.nowFn(), mutableSeg)}, b.activeSegments...)
}

// mutableActiveSegmentWithRLock returns any activeSegment marked as a mutableSegment.
// NB: it can return nil if no such segment exists.
func (b *block) mutableActiveSegmentWithRLock() *activeSegment {
	for _, seg := range b.activeSegments {
		if seg.segmentType == segments.MutableType && seg.writable {
			return seg
		}
	}
	return nil
}

func (b *block) Stats() BlockStats {
	b.RLock()

	// active segments
	active := ActiveSegmentsStats{
		NumActiveCompactions: b.numActiveCompactions.Load(),
	}
	for _, seg := range b.activeSegments {
		active.NumDocs += seg.Size()
		active.NumSegments++
		if seg.compacting {
			active.NumSegmentsCompacting++
		}
		if seg.segmentType == segments.FSTType {
			active.NumFSTSegments++
		} else {
			active.NumMutableSegments++
		}
	}

	// all other segments
	shard := ShardRangeSegmentsStats{}
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			shard.NumSegments++
			shard.NumDocs += seg.Size()
		}
	}
	b.RUnlock()

	return BlockStats{
		Active: active,
		Shard:  shard,
	}
}

type blockMetrics struct {
	CompactionLatency     tally.Timer
	InsertEndToEndLatency tally.Timer
}

func newBlockMetrics(iopts instrument.Options) blockMetrics {
	scope := iopts.MetricsScope()
	return blockMetrics{
		CompactionLatency:     scope.Timer("compaction-latency"),
		InsertEndToEndLatency: scope.Timer("insert-end-to-end-latency"),
	}
}

func (b *block) writeBatchErrorInvalidState(state blockState) error {
	switch state {
	case blockStateClosed:
		return errUnableToWriteBlockClosed
	case blockStateSealed:
		return errUnableToWriteBlockSealed
	default: // should never happen
		err := fmt.Errorf(errUnableToWriteBlockUnknownStateFmtString, state)
		instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(err.Error())
		return err
	}
}

func (b *block) unknownWriteBatchInvariantError(err error) error {
	wrappedErr := fmt.Errorf("unexpected non-BatchPartialError from m3ninx InsertBatch: %v", err)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(wrappedErr.Error())
	return wrappedErr
}

func (b *block) bootstrappingSealedMutableSegmentInvariant(err error) error {
	wrapped := fmt.Errorf("internal error: bootstrapping a mutable segment already marked sealed: %v", err)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(wrapped.Error())
	return wrapped
}

func (b *block) openBlockHasNilActiveSegmentInvariantErrorWithRLock() error {
	err := fmt.Errorf("internal error: block has open block state [%v] has no mutable active segment", b.state)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(err.Error())
	return err
}

// blockShardsSegments is a collection of segments that has a mapping of what shards
// and time ranges they completely cover, this can only ever come from computing
// from data that has come from shards, either on an index flush or a bootstrap.
type blockShardRangesSegments struct {
	readsWg         sync.WaitGroup
	shardTimeRanges result.ShardTimeRanges
	segments        []segment.Segment
	sealed          bool
}

func (b *blockShardRangesSegments) Close() {
	b.sealed = true
	go func() {
		b.readsWg.Wait()
		for _, seg := range b.segments {
			if err := seg.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error closing segment: %v\n", err)
			}
		}
	}()
}

type closable interface {
	Close() error
}

type safeCloser struct {
	closable
	closed bool
}

func (c *safeCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.closable.Close()
}
