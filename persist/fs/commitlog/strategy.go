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

import "fmt"

// Strategy describes the commit log writing strategy
type Strategy int

const (
	// StrategyWriteWait describes the strategy that waits
	// for the buffered commit log chunk that contains a write to flush
	// before acknowledging a write
	StrategyWriteWait Strategy = iota

	// StrategyWriteBehind describes the strategy that does not wait
	// for the buffered commit log chunk that contains a write to flush
	// before acknowledging a write
	StrategyWriteBehind
)

// ValidStrategies returns the valid commit log strategies.
func ValidStrategies() []Strategy {
	return []Strategy{StrategyWriteWait, StrategyWriteBehind}
}

func (p Strategy) String() string {
	switch p {
	case StrategyWriteWait:
		return "write_wait"
	case StrategyWriteBehind:
		return "write_behind"
	}
	return "unknown"
}

// ValidateStrategy validates a commit log strategy.
func ValidateStrategy(v Strategy) error {
	validStrategy := false
	for _, valid := range ValidStrategies() {
		if valid == v {
			validStrategy = true
			break
		}
	}
	if !validStrategy {
		return fmt.Errorf("invalid strategy '%s' valid types are: %v",
			v.String(), ValidStrategies())
	}
	return nil
}

// UnmarshalYAML unmarshals an CachePolicy into a valid type from string.
func (p *Strategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return nil // None specified
	}
	for _, valid := range ValidStrategies() {
		if str == valid.String() {
			*p = valid
			return nil
		}
	}
	return fmt.Errorf("invalid strategy '%s' valid types are: %v",
		str, ValidStrategies())
}
