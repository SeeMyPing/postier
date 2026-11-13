// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Content encodings of message_contents.mime.
const (
	EncodingZstd     = "zstd"
	EncodingIdentity = "identity"
)

// maxContentSize caps what DecodeContent will inflate, whatever the
// compressed size claims: 64 MiB is above the largest message Postier
// accepts (25 MB by default).
const maxContentSize = 64 << 20

var (
	zstdOnce sync.Once
	zstdEnc  *zstd.Encoder
	zstdDec  *zstd.Decoder
)

func zstdInit() {
	zstdOnce.Do(func() {
		// A nil writer/reader gives the stateless EncodeAll/DecodeAll API,
		// safe for concurrent use.
		zstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		zstdDec, _ = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxContentSize))
	})
}

// EncodedContent is a MIME body ready to be stored.
type EncodedContent struct {
	Encoding string
	Data     []byte
	SHA256   []byte // of the original MIME
	Size     int    // of the original MIME
}

// EncodeContent compresses a MIME body for message_contents. Bodies are
// replicated synchronously through the WAL, so their size is paid twice;
// HTML email compresses by a factor of 4 to 10.
//
// The body stays in Postgres until v1 (decision D3). Moving large bodies to
// object storage will happen behind this pair of functions.
func EncodeContent(mime []byte) EncodedContent {
	zstdInit()
	sum := sha256.Sum256(mime)
	return EncodedContent{
		Encoding: EncodingZstd,
		Data:     zstdEnc.EncodeAll(mime, make([]byte, 0, len(mime)/4)),
		SHA256:   sum[:],
		Size:     len(mime),
	}
}

// DecodeContent returns the original MIME body.
func DecodeContent(encoding string, data []byte) ([]byte, error) {
	switch encoding {
	case EncodingIdentity:
		return data, nil
	case EncodingZstd:
		zstdInit()
		out, err := zstdDec.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("content: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("content: unknown encoding %q", encoding)
	}
}
