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
	"fmt"
	"regexp/syntax"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureSyntaxPerlTreatsAnchorsAsTextTerminator(t *testing.T) {
	// Test to ensure future compatibility with changes in `regexp/syntax`.
	//
	// We require that '^' and '$' only match input terminating characters (i.e.
	// text boundaries, not line boundaries within the input). The line of code
	// below ensures that syntax.Perl does the same.
	require.NotZero(t, syntax.Perl&syntax.OneLine)

	// ensure our `parseRegexp` internal function uses the right flags too.
	re, err := parseRegexp(".*")
	require.NoError(t, err)
	require.NotZero(t, re.Flags&syntax.OneLine)
}

func TestEnsureRegexpUnachored(t *testing.T) {
	testCases := []testCase{
		testCase{
			name:           "naked ^",
			input:          "^",
			expectedOutput: "(?:)",
		},
		testCase{
			name:           "naked $",
			input:          "$",
			expectedOutput: "(?:)",
		},
		testCase{
			name:           "empty string ^$",
			input:          "^$",
			expectedOutput: "",
		},
		testCase{
			name:           "invalid naked concat ^$",
			input:          "$^",
			expectedOutput: "(?-m:$)\\A",
		},
		testCase{
			name:           "simple case of ^",
			input:          "^abc",
			expectedOutput: "abc",
		},
		testCase{
			name:           "simple case of $",
			input:          "abc$",
			expectedOutput: "abc",
		},
		testCase{
			name:           "simple case of both ^ & $",
			input:          "^abc$",
			expectedOutput: "abc",
		},
		testCase{
			name:           "weird case of internal ^",
			input:          "^a^bc$",
			expectedOutput: "a\\Abc",
		},
		testCase{
			name:           "weird case of internal $",
			input:          "^a$bc$",
			expectedOutput: "a(?-m:$)bc",
		},
		testCase{
			name:           "alternate of sub expressions with only legal ^ and $",
			input:          "(?:^abc$)|(?:^xyz$)",
			expectedOutput: "abc|xyz",
		},
		testCase{
			name:           "concat of sub expressions with only legal ^ and $",
			input:          "(?:^abc$)(?:^xyz$)",
			expectedOutput: "abc(?-m:$)\\Axyz",
		},
		testCase{
			name:           "alternate of sub expressions with illegal ^ and $",
			input:          "(?:^a$bc$)|(?:^xyz$)",
			expectedOutput: "a(?-m:$)bc|xyz",
		},
		testCase{
			name:           "concat of sub expressions with illegal ^ and $",
			input:          "(?:^a$bc$)(?:^xyz$)",
			expectedOutput: "a(?-m:$)bc(?-m:$)\\Axyz",
		},
		testCase{
			name:           "question mark case both boundaries success",
			input:          "(?:^abc$)?",
			expectedOutput: "(?:abc)?",
		},
		testCase{
			name:           "question mark case only ^",
			input:          "(?:^abc)?",
			expectedOutput: "(?:abc)?",
		},
		testCase{
			name:           "question mark case only $",
			input:          "(?:abc$)?",
			expectedOutput: "(?:abc)?",
		},
		testCase{
			name:           "question concat case $",
			input:          "abc$?",
			expectedOutput: "abc",
		},
		testCase{
			name:           "star mark case both boundaries success",
			input:          "(?:^abc$)*",
			expectedOutput: "(?:abc)?(?:\\Aabc(?-m:$))*",
		},
		testCase{
			name:           "star mark case only ^",
			input:          "(?:^abc)*",
			expectedOutput: "(?:abc)?(?:\\Aabc)*",
		},
		testCase{
			name:           "star mark case only $",
			input:          "(?:abc$)*",
			expectedOutput: "(?:abc)?(?:abc(?-m:$))*",
		},
		testCase{
			name:           "star concat case $",
			input:          "abc$*",
			expectedOutput: "abc(?-m:$)*",
		},
		testCase{
			name:           "star concat case ^",
			input:          "^*abc",
			expectedOutput: "\\A*abc",
		},
		testCase{
			name:           "plus mark case both boundaries success",
			input:          "(?:^abc$)+",
			expectedOutput: "abc(?:\\Aabc(?-m:$))*",
		},
		testCase{
			name:           "plus mark case only ^",
			input:          "(?:^abc)+",
			expectedOutput: "abc(?:\\Aabc)*",
		},
		testCase{
			name:           "plus mark case only $",
			input:          "(?:abc$)+",
			expectedOutput: "abc(?:abc(?-m:$))*",
		},
		testCase{
			name:           "plus concat case $",
			input:          "abc$+",
			expectedOutput: "abc(?-m:$)*",
		},
		testCase{
			name:           "plus concat case ^",
			input:          "^+abc",
			expectedOutput: "\\A*abc",
		},
		testCase{
			name:           "repeat case both boundaries success",
			input:          "(?:^abc$){3,4}",
			expectedOutput: "abc(?:\\Aabc(?-m:$)){2,3}",
		},
	}
	for _, tc := range testCases {
		re, err := parseRegexp(tc.input)
		require.NoError(t, err)
		parsed, err := ensureRegexpUnanchored(re)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedOutput, parsed.String(), tc.name)
	}
}

func TestEnsureRegexpAnchored(t *testing.T) {
	testCases := []testCase{
		testCase{
			name:           "naked ^",
			input:          "(?:)",
			expectedOutput: "\\A\\z",
		},
		testCase{
			name:           "invalid naked concat ^$",
			input:          "$^",
			expectedOutput: "\\A(?-m:$)\\A\\z",
		},
		testCase{
			name:           "simple case of literal",
			input:          "abc",
			expectedOutput: "\\Aabc\\z",
		},
		testCase{
			name:           "weird case of internal ^",
			input:          "a^bc",
			expectedOutput: "\\Aa\\Abc\\z",
		},
		testCase{
			name:           "weird case of internal $",
			input:          "a$bc",
			expectedOutput: "\\Aa(?-m:$)bc\\z",
		},
		testCase{
			name:           "alternate of sub expressions with only legal ^ and $",
			input:          "abc|xyz",
			expectedOutput: "\\A(?:abc|xyz)\\z",
		},
		testCase{
			name:           "concat of sub expressions with only legal ^ and $",
			input:          "(?:abc)(?:xyz)",
			expectedOutput: "\\Aabcxyz\\z",
		},
		testCase{
			name:           "question mark case both boundaries success",
			input:          "(?:abc)?",
			expectedOutput: "\\A(?:abc)?\\z",
		},
		testCase{
			name:           "star mark case both boundaries success",
			input:          "(?:abc)*",
			expectedOutput: "\\A(?:abc)*\\z",
		},
		testCase{
			name:           "plus mark case both boundaries success",
			input:          "(?:abc)+",
			expectedOutput: "\\A(?:abc)+\\z",
		},
		testCase{
			name:           "repeat case both boundaries success",
			input:          "(?:abc){3,4}",
			expectedOutput: "\\A(?:abc){3,4}\\z",
		},
	}
	for _, tc := range testCases {
		re, err := parseRegexp(tc.input)
		require.NoError(t, err)
		parsed, err := ensureRegexpAnchored(re)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedOutput, parsed.String(), tc.name)
	}
}

type testCase struct {
	name           string
	input          string
	expectedOutput string
}

// nolint
// only used for debugging
func pprintAst(ast *syntax.Regexp) {
	println(fmt.Sprintf("%+v", *ast))
	for i, s := range ast.Sub {
		println(fmt.Sprintf("%d>", i))
		pprintAst(s)
	}
}
