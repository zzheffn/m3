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

package commitlog

import (
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
)

const (
	// CommitLogBootstrapperName is the name of the commit log bootstrapper.
	CommitLogBootstrapperName = "commitlog"
)

type commitLogBootstrapper struct {
	bootstrap.Bootstrapper
}

// FilesystemInspection contains the outcome of a filesystem inspection
type FilesystemInspection struct {
	// orderedCommitlogFiles contains all commitlog filenames that existed
	// before the node began accepting writes.
	orderedCommitlogFiles []string
}

// CommitlogFilesSet generates a set of unique commitlog files
func (f FilesystemInspection) CommitlogFilesSet() map[string]struct{} {
	set := map[string]struct{}{}
	for _, file := range f.orderedCommitlogFiles {
		set[file] = struct{}{}
	}
	return set
}

// NewCommitLogBootstrapper creates a new bootstrapper to bootstrap from commit log files.
func NewCommitLogBootstrapper(
	opts Options,
	inspection FilesystemInspection,
	next bootstrap.Bootstrapper,
) (bootstrap.Bootstrapper, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	src := newCommitLogSource(opts, inspection)
	b := &commitLogBootstrapper{}
	b.Bootstrapper = bootstrapper.NewBaseBootstrapper(b.String(),
		src, opts.ResultOptions(), next)
	return b, nil
}

func (*commitLogBootstrapper) String() string {
	return CommitLogBootstrapperName
}

// InspectFilesystem scans the filesystem and generates a FilesystemInspection
// which the commitlog bootstrapper needs to avoid reading commitlog files that
// were written *after* the process has already started. I.E in order to distinguish
// between files that were already on disk before the process started, and those that
// were written by the process itself once it started accepting writes (but before
// bootstrapping had complete) we export a function which can be called during node
// startup.
func InspectFilesystem(fsOpts fs.Options) (FilesystemInspection, error) {
	path := fs.CommitLogsDirPath(fsOpts.FilePathPrefix())
	files, err := fs.CommitLogFiles(path)
	if err != nil {
		return FilesystemInspection{}, err
	}

	return FilesystemInspection{
		orderedCommitlogFiles: files,
	}, nil
}
