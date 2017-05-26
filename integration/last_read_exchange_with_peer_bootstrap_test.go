// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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

package integration

import (
	"testing"
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	xlog "github.com/m3db/m3x/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test ensures that block last read times are exchanged
// during a peer bootstrap.
func TestLastReadExchangeWithPeerBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setups
	log := xlog.SimpleLogger
	namesp := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
	opts := newTestOptions().
		SetNamespaces([]namespace.Metadata{namesp})

	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(8 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute).
		SetBufferDrain(time.Minute)
	setupOpts := []bootstrappableTestSetupOptions{
		{disablePeersBootstrapper: true},
		{disablePeersBootstrapper: false},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts,
		retentionOpts, setupOpts)
	defer closeFn()

	// Write test data for first node
	now := setups[0].getNowFn() // now is already truncate to block size
	blockSize := setups[0].storageOpts.RetentionOptions().BlockSize()
	start, end := now.Add(-3*blockSize), now
	seriesMaps := generateTestDataByStart([]testData{
		{ids: []string{"foo", "bar"}, numPoints: 50, start: now.Add(-3 * blockSize)},
		{ids: []string{"foo", "baz"}, numPoints: 50, start: now.Add(-2 * blockSize)},
		{ids: []string{"foo", "qux"}, numPoints: 50, start: now.Add(-1 * blockSize)},
	})
	err := writeTestDataToDisk(t, namesp.ID(), setups[0], seriesMaps)
	require.NoError(t, err)

	// Stop the servers at completion
	defer func() {
		log.Debug("servers shutting down")
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())
	log.Debug("first server is now up")

	// Issue initial reads
	_, err = m3dbClientFetch(setups[0].m3dbClient, &rpc.FetchRequest{
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		RangeStart: start.Unix(),
		RangeEnd:   start.Add(2 * blockSize).Unix(),
		NameSpace:  namesp.ID().String(),
		ID:         "foo",
	})
	assert.NoError(t, err)

	_, err = m3dbClientFetch(setups[0].m3dbClient, &rpc.FetchRequest{
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		RangeStart: start.Unix(),
		RangeEnd:   start.Add(2 * blockSize).Unix(),
		NameSpace:  namesp.ID().String(),
		ID:         "bar",
	})
	assert.NoError(t, err)

	// Verify last read times
	expectedReadState := lastReadState{
		ids: map[string]lastReadIDState{
			"foo": {blocks: map[time.Time]lastReadBlockState{
				start: {lastRead: end},
				start.Add(1 * blockSize): {lastRead: end},
			}},
			"bar": {blocks: map[time.Time]lastReadBlockState{
				start: {lastRead: end},
			}},
		},
	}

	log.Debug("verifying initial reads on first server")
	verifyLastReads(t, setups[0], namesp.ID(), start, end, expectedReadState)

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].startServer())
	log.Debug("both servers are now up")

	// Verify last read times exchanged
	log.Debug("verifying last read metadata exchanged")
	setups.parallel(func(s *testSetup) {
		verifyLastReads(t, s, namesp.ID(), start, end, expectedReadState)
	})

	log.Debug("done verification")
}
