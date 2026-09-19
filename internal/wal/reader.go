package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// ErrTorn reports a record that could not be read whole and intact.
//
// At the very end of the last segment this is the ordinary result of a crash — the process
// died between the write and the fsync — and the recovery path truncates and carries on.
// Anywhere else it is real corruption, and the caller must refuse to start rather than
// silently serve a file with a hole in it. Distinguishing those two is the reader's whole job;
// deciding what they mean is Open's.
var ErrTorn = errors.New("wal: torn or corrupt record")

// Reader iterates the records of one segment.
//
// It is used both by recovery at boot and by the compactor, which is deliberate: a bug in this
// scan would otherwise show up in only one of them, and the compactor's copy would be the one
// nobody tested against a crash.
type Reader struct {
	f   *os.File
	br  *bufio.Reader
	off int64 // offset of the next record to be read
}

func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f, br: bufio.NewReaderSize(f, 1<<20)}, nil
}

// Offset is where the record most recently attempted began. After ErrTorn this is the
// truncation point: everything before it is intact.
func (r *Reader) Offset() int64 { return r.off }

// Next returns the next record's payload, io.EOF at a clean end, or ErrTorn.
//
// The returned slice is freshly allocated and owned by the caller. Reusing one buffer would be
// faster and is a trap: the compactor holds records while it builds a Parquet row group, so a
// shared buffer would hand it rows that change underneath it.
func (r *Reader) Next() ([]byte, error) {
	var hdr [headerSize]byte

	n, err := io.ReadFull(r.br, hdr[:])
	switch {
	case errors.Is(err, io.EOF) && n == 0:
		// Clean end: the previous record finished exactly at the end of the file.
		return nil, io.EOF
	case err != nil:
		// A partial header is a torn tail, not a clean end.
		return nil, ErrTorn
	}

	length := binary.LittleEndian.Uint32(hdr[0:4])
	want := binary.LittleEndian.Uint32(hdr[4:8])

	if length == 0 || length > maxRecordBytes {
		return nil, ErrTorn
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r.br, payload); err != nil {
		return nil, ErrTorn
	}

	if crc32.Checksum(payload, castagnoli) != want {
		return nil, ErrTorn
	}

	r.off += headerSize + int64(length)
	return payload, nil
}

func (r *Reader) Close() error { return r.f.Close() }
