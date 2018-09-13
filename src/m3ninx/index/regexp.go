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
	re "regexp"
	"regexp/syntax"

	fstregexp "github.com/m3db/m3/src/m3ninx/index/segment/fst/regexp"
)

// CompileRegex compiles the provided regexp into an object that can be used to query the various
// segment implementations.
func CompileRegex(r []byte) (CompiledRegex, error) {
	// NB(prateek): We currently use two segment implementations: map-backed, and fst-backed (Vellum).
	// Due to peculiarities in the implementation of Vellum, we have to make certain modifications
	// to all incoming regular expressions to ensure compatibility between them.

	// first, we parse the regular expression into the equivalent regex
	reString := string(r)
	reAst, err := parseRegexp(reString)
	if err != nil {
		return CompiledRegex{}, err
	}

	// Issue (a): Vellum does not allow regexps which use characters '^', or '$'.
	// To address this issue, we strip these characters from appropriate locations in the parsed syntax.Regexp
	// for Vellum's RE.
	vellumRe, err := ensureRegexpUnanchored(reAst)
	if err != nil {
		return CompiledRegex{}, fmt.Errorf("unable to create FST re: %v", err)
	}

	// Issue (b): Vellum treats every regular expression as anchored, where as the map-backed segment does not.
	// To address this issue, we ensure that every incoming regular expression is modified to be anchored
	// when querying the map-backed segment, and isn't anchored when querying Vellum's RE.
	simpleRe, err := ensureRegexpAnchored(vellumRe)
	if err != nil {
		return CompiledRegex{}, fmt.Errorf("unable to create map re: %v", err)
	}

	simpleRE, err := re.Compile(simpleRe.String())
	if err != nil {
		return CompiledRegex{}, err
	}
	compiledRegex := CompiledRegex{Simple: simpleRE}

	fstRE, start, end, err := fstregexp.ParsedRegexp(reString, vellumRe)
	if err != nil {
		return CompiledRegex{}, err
	}
	compiledRegex.FST = fstRE
	compiledRegex.PrefixBegin = start
	compiledRegex.PrefixEnd = end

	return compiledRegex, nil
}

func parseRegexp(re string) (*syntax.Regexp, error) {
	return syntax.Parse(re, syntax.Perl)
}

// ensureRegexpAnchored adds '^' and '$' characters to appropriate locations in the parsed syntax.Regexp,
// to ensure every input regular expression is converted to it's equivalent anchored regular expression.
// NB: assumes input regexp AST is un-anchored.
func ensureRegexpAnchored(unanchoredRegexp *syntax.Regexp) (*syntax.Regexp, error) {
	ast := &syntax.Regexp{
		Op:    syntax.OpConcat,
		Flags: syntax.Perl,
		Sub: []*syntax.Regexp{
			&syntax.Regexp{
				Op:    syntax.OpBeginText,
				Flags: syntax.Perl,
			},
			unanchoredRegexp,
			&syntax.Regexp{
				Op:    syntax.OpEndText,
				Flags: syntax.Perl,
			},
		},
	}
	return simplify(ast), nil
}

// ensureRegexpUnanchored strips '^' and '$' characters from appropriate locations in the parsed syntax.Regexp,
// to ensure every input regular expression is converted to it's equivalent un-anchored regular expression
// assuming the entire input is matched.
func ensureRegexpUnanchored(parsed *syntax.Regexp) (*syntax.Regexp, error) {
	r, _, err := ensureRegexpUnanchoredHelper(parsed, true, true)
	if err != nil {
		return nil, err
	}
	return simplify(r), nil
}

func ensureRegexpUnanchoredHelper(parsed *syntax.Regexp, leftmost, rightmost bool) (output *syntax.Regexp, changed bool, err error) {
	// short circuit when we know we won't make any changes to the underlying regexp.
	if !leftmost && !rightmost {
		return parsed, false, nil
	}

	newRegexp, changed := parsed, false
	switch parsed.Op {
	case syntax.OpBeginLine, syntax.OpEndLine:
		// i.e. the flags provided to syntax.Parse did not include the `OneLine` flag, which
		// should never happen as we're using syntax.Perl which does include it (ensured by a test
		// in this package).
		return nil, false, fmt.Errorf("regular expressions are forced to be single line")
	case syntax.OpBeginText:
		if leftmost {
			newRegexp = &syntax.Regexp{
				Op:    syntax.OpEmptyMatch,
				Flags: parsed.Flags,
			}
			changed = true
		}
	case syntax.OpEndText:
		if rightmost {
			newRegexp = &syntax.Regexp{
				Op:    syntax.OpEmptyMatch,
				Flags: parsed.Flags,
			}
			changed = true
		}
	case syntax.OpConcat:
		// strip left-most '^'
		if l := len(parsed.Sub); leftmost && l > 0 {
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[0], leftmost, rightmost && l == 1)
			if err != nil {
				return nil, false, err
			}
			if c {
				parsed.Sub[0] = newRe
				changed = true
			}
		}
		// strip right-most '$'
		if l := len(parsed.Sub); rightmost && l > 0 {
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[l-1], leftmost && l == 1, rightmost)
			if err != nil {
				return nil, false, err
			}
			if c {
				parsed.Sub[l-1] = newRe
				changed = true
			}
		}
	case syntax.OpAlternate:
		// strip left-most '^' and right-most '$' in each sub-expression
		for idx := range parsed.Sub {
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[idx], leftmost, rightmost)
			if err != nil {
				return nil, false, err
			}
			parsed.Sub[idx] = newRe
			changed = changed || c
		}
	case syntax.OpQuest:
		if len(parsed.Sub) > 0 {
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[0], leftmost, rightmost)
			if err != nil {
				return nil, false, err
			}
			if c {
				parsed.Sub[0] = newRe
				changed = true
			}
		}
	case syntax.OpStar:
		if len(parsed.Sub) > 0 {
			original := deepCopy(parsed)
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[0], leftmost, rightmost)
			if err != nil {
				return nil, false, err
			}
			if !c {
				return parsed, false, nil
			}
			newRegexp = &syntax.Regexp{
				Op:    syntax.OpConcat,
				Flags: parsed.Flags,
				Sub: []*syntax.Regexp{
					&syntax.Regexp{
						Op:    syntax.OpQuest,
						Flags: parsed.Flags,
						Sub: []*syntax.Regexp{
							newRe,
						},
					},
					original,
				},
			}
			changed = true
		}
	case syntax.OpPlus:
		if len(parsed.Sub) > 0 {
			original := deepCopy(parsed)
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[0], leftmost, rightmost)
			if err != nil {
				return nil, false, err
			}
			if !c {
				return parsed, false, nil
			}
			return &syntax.Regexp{
				Op:    syntax.OpConcat,
				Flags: parsed.Flags,
				Sub: []*syntax.Regexp{
					newRe,
					&syntax.Regexp{
						Op:    syntax.OpStar,
						Flags: parsed.Flags,
						Sub: []*syntax.Regexp{
							original.Sub[0],
						},
					},
				},
			}, true, nil
		}
	case syntax.OpRepeat:
		if len(parsed.Sub) > 0 && parsed.Min > 0 && parsed.Max > 0 {
			original := deepCopy(parsed)
			newRe, c, err := ensureRegexpUnanchoredHelper(parsed.Sub[0], leftmost, rightmost)
			if err != nil {
				return nil, false, err
			}
			if !c {
				return parsed, false, nil
			}
			original.Min--
			original.Max--
			newRegexp = &syntax.Regexp{
				Op:    syntax.OpConcat,
				Flags: parsed.Flags,
				Sub: []*syntax.Regexp{
					newRe,
					original,
				},
			}
			changed = true
		}
	}
	return newRegexp, changed, nil
}

func deepCopy(ast *syntax.Regexp) *syntax.Regexp {
	if ast == nil {
		return nil
	}
	copied := *ast
	copied.Sub = make([]*syntax.Regexp, 0, len(ast.Sub))
	for _, r := range ast.Sub {
		copied.Sub = append(copied.Sub, deepCopy(r))
	}
	if len(copied.Sub0) != 0 && copied.Sub0[0] != nil {
		copied.Sub0[0] = deepCopy(copied.Sub0[0])
	}
	return &copied
}

var emptyStringRes = []*syntax.Regexp{
	&syntax.Regexp{Op: syntax.OpEmptyMatch},
	&syntax.Regexp{Op: syntax.OpQuest, Sub: []*syntax.Regexp{&syntax.Regexp{Op: syntax.OpEmptyMatch}}},
	&syntax.Regexp{Op: syntax.OpPlus, Sub: []*syntax.Regexp{&syntax.Regexp{Op: syntax.OpEmptyMatch}}},
	&syntax.Regexp{Op: syntax.OpStar, Sub: []*syntax.Regexp{&syntax.Regexp{Op: syntax.OpEmptyMatch}}},
}

func matchesEmptyString(ast *syntax.Regexp) bool {
	if ast == nil {
		return false
	}
	for _, empty := range emptyStringRes {
		if ast.Equal(empty) {
			return true
		}
	}
	return false
}

func simplify(ast *syntax.Regexp) *syntax.Regexp {
	if ast == nil {
		return nil
	}
	switch ast.Op {
	case syntax.OpConcat:
		subs := make([]*syntax.Regexp, 0, len(ast.Sub))
		for _, sub := range ast.Sub {
			s := simplify(sub)
			if !matchesEmptyString(s) {
				subs = append(subs, simplify(s))
			}
		}
		ast.Sub = subs
	default:
		for idx := range ast.Sub {
			ast.Sub[idx] = simplify(ast.Sub[idx])
		}
	}
	return ast
}
