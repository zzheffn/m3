// Copyright (c) 2016 Uber Technologies, Inc.
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

package encoding

import (
	"testing"
	"time"

	"github.com/m3db/m3x/checked"
	xtime "github.com/m3db/m3x/time"

	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
)

type SeriesBlocks struct {
	SeriesID ident.ID
	Blocks []SeriesBlock
}

type SeriesBlock struct {
	Start time.Time
	End time.Time
	Replicas []MultiReaderIterator
}

func TestDeconstructAndReconstruct(t *testing.T) {
	now := time.Now()
	start := now.Truncate(time.Hour)
	end := start.Add(30 * time.Minute)

	encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil), true, NewOptions())
	encoder.Encode(ts.Datapoint{Timestamp: start, Value: 42}, xtime.Second, nil)

	iterAlloc := func(r io.Reader) encoding.ReaderIterator {
		iter := m3tsz.NewDecoder(true, NewOptions())
		return iter.Decode(r)
	}

	multiReader := NewMultiReaderIterator(iterAlloc, nil)

	orig := NewSeriesIterator(ident.StringID("namespace"), ident.StringID("foo"), start, end, []MultiReaderIterator{
		multiReader
	})

	series := SeriesBlocks{
		SeriesID: ident.StringID("foo")
	}

	for _, replica := range orig.Replicas() {
		perBlockSliceReaders := replica.Readers()
		for perBlockSliceReaders.Next() {
			// we are at a block
			start := perBlockSliceReaders.CurrentStart()
			end := perBlockSliceReaders.CurrentEnd()

			var readers []io.Reader
			for i := 0; i < perBlockSliceReaders.CurrentLen(); i++ {
				// reader to an unmerged (or already merged) block buffer
				readers = append(readers, perBlockSliceReaders.CurrentAt(i))
			}

			iter := NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, end)

			inserted := false
			for i := range series.Blocks {
				if series.Blocks[i].Start.Equal(start) {
					inserted = true
					series.Blocks[i].Replicas = append(series.Blocks[i].Replicas, iter)
					break
				}
			}
			if !insert {
				series.Blocks = append(series.Blocks, SeriesBlock{
					Start: start,
					End: end,
					Replicas: []MultiReaderIterator{iter},
				})
			}
		}
	}


	for _, block := range series.Blocks {
		start := block.Start
		end := block.End
		filterValuesStart := orig.Start() // should actually be max(orig.Start, block.Start)
		filterValuesEnd := orig.End() // should actually be min(orig.End(), block.End)
		blockIter := NewSeriesIterator(orig.Namespace(), orig.ID(),
			filterValuesStart, filterValuesEnd, block.Replicas, nil)
	}
}
