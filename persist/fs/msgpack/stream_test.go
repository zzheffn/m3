// Copyright (c) 2017 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package msgpack

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Call Read to accumulate the text of a file
func reads(buf DecoderStream, m int) string {
	var b [1000]byte
	if int(buf.Remaining()) > len(b) {
		panic(fmt.Errorf("cannot read all"))
	}

	nb := 0
	for {
		n, err := buf.Read(b[nb : nb+m])
		nb += n
		if err == io.EOF {
			break
		}
	}
	return string(b[0:nb])
}

func TestDecoderStream(t *testing.T) {
	var texts [31]string
	str := ""
	all := ""
	for i := 0; i < len(texts)-1; i++ {
		texts[i] = str + "\n"
		all += texts[i]
		str += string(i%26 + 'a')
	}
	texts[len(texts)-1] = all

	buf := NewDecoderStream(nil)
	for i := 0; i < len(texts); i++ {
		text := texts[i]
		for j := 1; j <= 8; j++ {
			buf.Reset([]byte(text))
			s := reads(buf, j)
			if s != text {
				t.Errorf("m=%d want=%q got=%q", j, text, s)
			}
		}
	}
}

func TestDecoderStreamSkip(t *testing.T) {
	d := []byte{1, 2, 3, 4, 5}
	buf := NewDecoderStream(d)
	assert.Equal(t, int64(5), buf.Remaining())
	assert.NoError(t, buf.Skip(3))
	assert.Equal(t, int64(2), buf.Remaining())

	p := make([]byte, 2)
	n, err := buf.Read(p)
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{4, 5}, p)
}

func TestDecoderStreamUnreadByte(t *testing.T) {
	segments := []string{"Hello, ", "world"}
	got := ""
	want := strings.Join(segments, "")
	r := NewDecoderStream([]byte(want))
	// Normal execution.
	for {
		b1, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				t.Error("unexpected error on ReadByte:", err)
			}
			break
		}
		got += string(b1)
		// Put it back and read it again.
		if err = r.UnreadByte(); err != nil {
			t.Fatal("unexpected error on UnreadByte:", err)
		}
		b2, err := r.ReadByte()
		if err != nil {
			t.Fatal("unexpected error reading after unreading:", err)
		}
		if b1 != b2 {
			t.Fatalf("incorrect byte after unread: got %q, want %q", b1, b2)
		}
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecoderStreamUnreadByteMultiple(t *testing.T) {
	segments := []string{"Hello, ", "world"}
	data := []byte(strings.Join(segments, ""))
	for n := 0; n <= len(data); n++ {
		r := NewDecoderStream(data)
		// Read n bytes.
		for i := 0; i < n; i++ {
			b, err := r.ReadByte()
			if err != nil {
				t.Fatalf("n = %d: unexpected error on ReadByte: %v", n, err)
			}
			if b != data[i] {
				t.Fatalf("n = %d: incorrect byte returned from ReadByte: got %q, want %q", n, b, data[i])
			}
		}
		// Unread one byte if there is one.
		if n > 0 {
			remaining := r.Remaining()
			if expect := int64(len(data) - n); remaining != expect {
				t.Errorf("n = %d: unexpected remaining before UnreadByte: got %d, want %d", n, remaining, expect)
			}
			if err := r.UnreadByte(); err != nil {
				t.Errorf("n = %d: unexpected error on UnreadByte: %v", n, err)
			}
			remaining = r.Remaining()
			if expect := int64(len(data) - n + 1); remaining != expect {
				t.Errorf("n = %d: unexpected remaining after UnreadByte: got %d, want %d", n, remaining, expect)
			}
		}
		// Test that we cannot unread any further.
		if err := r.UnreadByte(); err == nil {
			t.Errorf("n = %d: expected error on UnreadByte", n)
		}
		// Test that it can be read back with Read.
		if n > 0 {
			var c [1]byte
			_, err := r.Read(c[:])
			if err != nil {
				t.Errorf("n = %d: unexpected error on Read after UnreadByte: %v", n, err)
			}
			if c[0] != data[n-1] {
				t.Errorf("n = %d: unexpected error on Read after UnreadByte: %v != %v", n, c[0], data[n-1])
			}
		}
	}
}