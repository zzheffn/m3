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
	"bytes"
	"fmt"
	"time"

	m3ninxindex "github.com/m3db/m3db/src/m3ninx/index"
	"github.com/m3db/m3db/src/m3ninx/index/segment"
	"github.com/m3db/m3db/src/m3ninx/index/segment/fs"
	m3ninxpersist "github.com/m3db/m3db/src/m3ninx/persist"
	"github.com/m3db/m3db/src/x/mmap"
)

type activeSegmentState byte

const (
	mutableActiveSegmentState  activeSegmentState = iota
	rotatingActiveSegmentState                    // i.e. fst is being created but is not ready for use yet
	fstActiveSegmentState
)

func (a activeSegmentState) String() string {
	switch a {
	case mutableActiveSegmentState:
		return "mutableActiveSegment"
	case rotatingActiveSegmentState:
		return "rotatingActiveSegment"
	case fstActiveSegmentState:
		return "fstActiveSegment"
	}
	return "unknownActiveSegment"
}

// activeSegment starts out backed by a mutable segment, which is rotated
// to a FST segment based on size constraints.
type activeSegment struct {
	creationTime   time.Time
	state          activeSegmentState
	mutableSegment segment.MutableSegment
	fstSegment     segment.Segment
}

func (a *activeSegment) TransformIntoFST(opts Options) error {
	if a.state != rotatingActiveSegmentState {
		return fmt.Errorf("unable to transform activeSegment with state: %v into FST", a.state)
	}

	writer, err := m3ninxpersist.NewMutableSegmentFileSetWriter()
	if err != nil {
		return err
	}

	if err := writer.Reset(a.mutableSegment); err != nil {
		return err
	}

	success := false
	fstData := &fstSegmentMetadata{
		major:    writer.MajorVersion(),
		minor:    writer.MinorVersion(),
		metadata: append([]byte{}, writer.SegmentMetadata()...),
	}
	// cleanup incase we run into issues
	defer func() {
		if !success {
			for _, f := range fstData.files {
				f.Close()
			}
		}
	}()

	var bytesWriter bytes.Buffer
	for _, f := range writer.Files() {
		bytesWriter.Reset()
		if err := writer.WriteFile(f, &bytesWriter); err != nil {
			return err
		}
		fileBytes := bytesWriter.Bytes()
		// memcpy bytes -> new mmap region to hide from the GC
		mmapedResult, err := mmap.Bytes(int64(len(fileBytes)), mmap.Options{
			Read:  true,
			Write: true, // TODO(prateek): pass down huge TLB constraints here?
		})
		if err != nil {
			return err
		}
		copy(mmapedResult.Result, fileBytes)
		fstData.files = append(fstData.files,
			m3ninxpersist.NewMmapedIndexSegmentFile(f, nil, mmapedResult.Result))
	}

	// NB: need to mark success here as the NewSegment call assumes ownership of
	// the provided bytes regardless of success/failure.
	success = true
	fstSegment, err := m3ninxpersist.NewSegment(fstData, fs.NewSegmentOpts{
		PostingsListPool: opts.MemSegmentOptions().PostingsListPool(),
	})
	if err != nil {
		return err
	}

	a.fstSegment = fstSegment
	return nil
}

func (a *activeSegment) Reader() (m3ninxindex.Reader, error) {
	if a.state == fstActiveSegmentState {
		return a.fstSegment.Reader()
	}
	return a.mutableSegment.Reader()
}

func (a *activeSegment) Size() int64 {
	if a.state == fstActiveSegmentState {
		return a.fstSegment.Size()
	}
	return a.mutableSegment.Size()
}

func (a *activeSegment) Close() error {
	if a.state == mutableActiveSegmentState {
		return a.mutableSegment.Close()
	}

	// TODO(prateek): handle rotatingActiveSegmentState
	return a.fstSegment.Close()
}

type fstSegmentMetadata struct {
	major    int
	minor    int
	metadata []byte
	files    []m3ninxpersist.IndexSegmentFile
}

var _ m3ninxpersist.IndexSegmentFileSet = &fstSegmentMetadata{}

func (f *fstSegmentMetadata) SegmentType() m3ninxpersist.IndexSegmentType {
	return m3ninxpersist.FSTIndexSegmentType
}

func (f *fstSegmentMetadata) MajorVersion() int       { return f.major }
func (f *fstSegmentMetadata) MinorVersion() int       { return f.minor }
func (f *fstSegmentMetadata) SegmentMetadata() []byte { return f.metadata }
func (f *fstSegmentMetadata) Files() []m3ninxpersist.IndexSegmentFile {
	return f.files
}
