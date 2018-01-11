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

package commitlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestStrategyUnmarshal(t *testing.T) {
	conf := struct {
		Strategy Strategy `yaml:"strategy"`
	}{}
	yamlBytes := []byte(`
strategy: write_behind
`)
	require.NoError(t, yaml.Unmarshal(yamlBytes, &conf))
	assert.Equal(t, StrategyWriteBehind, conf.Strategy)
}

func TestStrategyUnmarshalDefault(t *testing.T) {
	conf := struct {
		Strategy Strategy `yaml:"strategy"`
	}{}
	require.NoError(t, yaml.Unmarshal(nil, &conf))
	assert.Equal(t, StrategyWriteWait, conf.Strategy)
}

func TestStrategyUnmarshalError(t *testing.T) {
	conf := struct {
		Strategy Strategy `yaml:"strategy"`
	}{}
	yamlBytes := []byte(`
strategy: invalid
`)
	assert.Error(t, yaml.Unmarshal(yamlBytes, &conf))
}

func TestStrategyValidate(t *testing.T) {
	assert.NoError(t, ValidateStrategy(StrategyWriteWait))
	assert.Error(t, ValidateStrategy(Strategy(-1)))
}
