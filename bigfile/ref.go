package bigfile

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
)

type CompressionCodec string

const (
	CompressNone   = CompressionCodec("none")
	CompressSnappy = CompressionCodec("snap")

	// CompressZlib   = CompressionCodec("zlib")
	// CompressGzip = CompressionCodec("gzip")
	// CompressZstd = CompressionCodec("zstd")
)

const DEKSize = 32

type DEK [DEKSize]byte

func (dek DEK) MarshalJSON() ([]byte, error) {
	return json.Marshal(dek[:])
}

func (dek *DEK) UnmarshalJSON(data []byte) error {
	var slice []byte
	if err := json.Unmarshal(data, &slice); err != nil {
		return err
	}
	copy(dek[:], slice)
	return nil
}

// RefSize is the size of a Ref marshalled to binary
const RefSize = cadata.IDSize + DEKSize + 4

// Ref is a reference to data in a content-addressed store
type Ref struct {
	CID      cadata.ID        `json:"cid"`
	DEK      DEK              `json:"dek"`
	Compress CompressionCodec `json:"compress"`
}

func RefFromBytes(x []byte) (*Ref, error) {
	if len(x) < RefSize {
		return nil, errors.Errorf("too small to be ref len=%d", len(x))
	}
	r := Ref{}
	br := bytes.NewReader(x)
	compressCodec := [4]byte{}
	for _, slice := range [][]byte{
		r.CID[:],
		r.DEK[:],
		compressCodec[:],
	} {
		if _, err := io.ReadFull(br, slice); err != nil {
			return nil, err
		}
	}
	r.Compress = CompressionCodec(compressCodec[:])
	return &r, nil
}

func (r Ref) MarshalBinary() ([]byte, error) {
	if len(r.Compress) != 4 {
		return nil, errors.Errorf("invalid CompressCodec %v", r.Compress)
	}
	buf := make([]byte, 0, RefSize)
	buf = append(buf, r.CID[:]...)
	buf = append(buf, r.DEK[:]...)
	buf = append(buf, []byte(r.Compress)...)
	return buf, nil
}

func (r Ref) GetCompression() CompressionCodec {
	if r.Compress == "" {
		return CompressNone
	}
	return r.Compress
}

func (r Ref) Key() (ret [32]byte) {
	for i := range ret {
		ret[i] = r.CID[i] ^ r.DEK[i]
	}
	return ret
}

func marshalRef(x Ref) []byte {
	data, _ := x.MarshalBinary()
	return data
}

func (o *Operator) post(ctx context.Context, s cadata.Store, salt *[32]byte, ptext []byte) (*Ref, error) {
	buf := o.acquireBuffer(len(ptext))
	defer o.releaseBuffer(buf)
	compressCodec, compData, err := o.compress(buf[:0], ptext)
	if err != nil {
		return nil, err
	}
	ctext := make([]byte, len(compData))
	dek := encrypt(salt, ctext, compData)
	cid, err := s.Post(ctx, ctext)
	if err != nil {
		return nil, err
	}
	return &Ref{
		CID:      cid,
		DEK:      dek,
		Compress: compressCodec,
	}, nil
}

func (o *Operator) getF(ctx context.Context, s cadata.Store, ref Ref, fn func([]byte) error) error {
	if value, ok := o.cache.Get(ref.Key()); ok {
		return fn(value.([]byte))
	}
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, ref.CID, buf)
	if err != nil {
		return err
	}
	if err := cadata.Check(s.Hash, ref.CID, buf[:n]); err != nil {
		return err
	}
	cryptoXOR(ref.DEK, buf[:n], buf[:n])
	data, err := decompress(ref.Compress, nil, buf[:n])
	if err != nil {
		return err
	}
	o.cache.Add(ref.Key(), data)
	return fn(data)
}

func (o *Operator) compress(out []byte, src []byte) (CompressionCodec, []byte, error) {
	codec := o.compCodec
	switch codec {
	case "", CompressNone:
		out = append(out, src...)
		return CompressNone, out, nil
	case CompressSnappy:
		out = snappy.Encode(out, src)
	default:
		panic(codec)
	}
	if len(out) >= len(src) {
		return CompressNone, src, nil
	}
	return codec, out, nil
}

func decompress(codec CompressionCodec, out, src []byte) ([]byte, error) {
	switch codec {
	case "", CompressNone:
		out = append(out, src...)
		return out, nil
	case CompressSnappy:
		return snappy.Decode(out, src)
	default:
		return nil, errors.Errorf("codec not recognized: %s", codec)
	}
}

func encrypt(salt *[32]byte, ctext, ptext []byte) DEK {
	if len(ctext) != len(ptext) {
		panic("len(ctext) != len(ptext)")
	}
	dek := makeDEK(salt, ptext)
	cryptoXOR(dek, ctext, ptext)
	return dek
}

func cryptoXOR(key DEK, dst, src []byte) {
	nonce := [chacha20.NonceSize]byte{} // 0
	ciph, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	ciph.XORKeyStream(dst, src)
}

func makeDEK(salt *[32]byte, ptext []byte) (dek DEK) {
	DeriveKey(dek[:], salt, ptext)
	return dek
}

// DeriveKey uses BLAKE3 keyed with salt, to absorb input, and derive a new key into out
func DeriveKey(out []byte, salt *[32]byte, input []byte) {
	h := blake3.New(len(out), salt[:])
	if _, err := h.Write(input); err != nil {
		panic(err)
	}
	xof := h.XOF()
	if _, err := io.ReadFull(xof, out); err != nil {
		panic(err)
	}
}

func dumpStore(x cadata.Store) string {
	sb := strings.Builder{}
	cadata.ForEach(context.TODO(), x, cadata.Span{}, func(x cadata.ID) error {
		sb.WriteString(x.String())
		sb.WriteString("\n")
		return nil
	})
	return sb.String()
}
