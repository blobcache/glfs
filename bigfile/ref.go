package bigfile

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/golang/snappy"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
)

type CompressionCodec string

const (
	CompressNone   = CompressionCodec("none")
	CompressZlib   = CompressionCodec("zlib")
	CompressSnappy = CompressionCodec("snap")

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

func post(ctx context.Context, s cadata.Store, salt []byte, ptext []byte) (*Ref, error) {
	compressCodec, compData, err := compress(CompressSnappy, ptext)
	if err != nil {
		return nil, err
	}
	ctext := make([]byte, len(compData))
	dek := encrypt(ctext, compData, salt)
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

var cache = newCache(64)

func newCache(size int) *lru.Cache {
	cache, err := lru.New(size)
	if err != nil {
		panic(err)
	}
	return cache
}

func get(ctx context.Context, s cadata.Store, ref Ref, fn func([]byte) error) error {
	if value, ok := cache.Get(ref.Key()); ok {
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
	data, err := decompress(ref.Compress, buf[:n])
	if err != nil {
		return err
	}
	cache.Add(ref.Key(), data)
	return fn(data)
}

func compress(codec CompressionCodec, src []byte) (CompressionCodec, []byte, error) {
	buf := bytes.Buffer{}
	switch codec {
	case "", CompressNone:
		return CompressNone, src, nil
	case CompressZlib:
		w := zlib.NewWriter(&buf)
		if _, err := w.Write(src); err != nil {
			return "", nil, err
		}
		if err := w.Close(); err != nil {
			return "", nil, err
		}
	case CompressSnappy:
		w := snappy.NewBufferedWriter(&buf)
		if _, err := w.Write(src); err != nil {
			return "", nil, err
		}
		if err := w.Close(); err != nil {
			return "", nil, err
		}
	default:
		panic(codec)
	}
	if buf.Len() >= len(src) {
		return CompressNone, src, nil
	}
	return codec, buf.Bytes(), nil
}

func decompress(codec CompressionCodec, src []byte) ([]byte, error) {
	switch codec {
	case "", CompressNone:
		return src, nil
	case CompressZlib:
		buf := bytes.NewReader(src)
		rc, err := zlib.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return ioutil.ReadAll(rc)
	case CompressSnappy:
		br := bytes.NewReader(src)
		sr := snappy.NewReader(br)
		return ioutil.ReadAll(sr)
	default:
		return nil, errors.Errorf("codec not recognized: %s", codec)
	}
}

func encrypt(ctext, ptext, salt []byte) DEK {
	if len(ctext) != len(ptext) {
		panic("len(ctext) != len(ptext)")
	}
	dek := deriveKey(salt, ptext)
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

func deriveKey(salt []byte, ptext []byte) DEK {
	h := blake3.New(32, nil)
	h.Write(salt)
	h.Write(ptext)
	dek := DEK{}
	h.Sum(dek[:0])
	return dek
}

func dumpStore(x cadata.Store) string {
	sb := strings.Builder{}
	cadata.ForEach(context.TODO(), x, func(x cadata.ID) error {
		sb.WriteString(x.String())
		sb.WriteString("\n")
		return nil
	})
	return sb.String()
}
