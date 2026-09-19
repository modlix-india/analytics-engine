// Package wal is an append-only write-ahead log with group commit.
//
// It is the engine's durability boundary and its queue. An ingest request is acknowledged once
// the batch containing it has been fsynced; the compactor later folds closed segments into
// Parquet. Holding events here rather than in a broker is what lets this run as one binary,
// and it is why adding Kafka later is a change of transport rather than of design.
//
// # Record format
//
//	┌──────────┬──────────┬──────────────┐
//	│ len  u32 │ crc32c   │ payload      │
//	└──────────┴──────────┴──────────────┘
//
// Little-endian, CRC over the payload alone. Castagnoli rather than IEEE because it has a
// hardware instruction on both amd64 and arm64, and this runs on every record written.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	// headerSize is len(u32) + crc32c(u32).
	headerSize = 8

	// maxRecordBytes bounds one record.
	//
	// This exists for recovery, not for writing. A corrupt length field is indistinguishable
	// from a valid one until the payload fails its CRC, so without a cap a single flipped
	// bit in a length could ask the reader to allocate gigabytes before it discovers the
	// record was junk. Refusing early turns that into a clean truncation.
	maxRecordBytes = 16 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// appendRecord encodes one record onto dst and returns the extended slice.
//
// It appends rather than writing so the caller can build a whole batch in one buffer and hand
// the file a single write, which is what keeps the syncer off the append path.
func appendRecord(dst, payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("wal: empty payload")
	}
	if len(payload) > maxRecordBytes {
		return nil, fmt.Errorf("wal: record of %d bytes exceeds the %d limit", len(payload), maxRecordBytes)
	}

	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))

	dst = append(dst, hdr[:]...)
	dst = append(dst, payload...)
	return dst, nil
}
