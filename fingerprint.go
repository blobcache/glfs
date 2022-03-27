package glfs

import (
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"lukechampine.com/blake3"
)

// Fingerprint is the type used to represent a fingerprint of user provided content.
// If two Refs have the same Fingerprint, then they should be considered equivalent.
type Fingerprint cadata.ID

func (fp Fingerprint) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(fp[:]))
}

func (fp *Fingerprint) UnmarshalJSON(data []byte) error {
	var hexStr string
	if err := json.Unmarshal(data, &hexStr); err != nil {
		return err
	}
	_, err := hex.Decode(fp[:], []byte(hexStr))
	return err
}

func (fp Fingerprint) String() string {
	return hex.EncodeToString(fp[:])
}

func (fp Fingerprint) IsZero() bool {
	return fp == (Fingerprint{})
}

// FPBytes creates a fingerprint of the bytes x
func FPBytes(x []byte) Fingerprint {
	return Fingerprint(blake3.Sum256(x))
}

// FPReader creates a fingerprint by reading bytes from r.
func FPReader(r io.Reader) (Fingerprint, error) {
	fpw := NewFPWriter()
	if _, err := io.Copy(fpw, r); err != nil {
		return Fingerprint{}, nil
	}
	return fpw.Finish(), nil
}

// FPWriter accumulates a Fingerprint based on all the data passed to Write
type FPWriter struct {
	h *blake3.Hasher
}

func NewFPWriter() *FPWriter {
	return &FPWriter{
		h: blake3.New(32, nil),
	}
}

func (w *FPWriter) Write(p []byte) (int, error) {
	return w.h.Write(p)
}

func (w *FPWriter) Finish() Fingerprint {
	var fp Fingerprint
	w.h.Sum(fp[:0])
	return fp
}

func xorFingerprint(out *Fingerprint, a, b Fingerprint) {
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
}
