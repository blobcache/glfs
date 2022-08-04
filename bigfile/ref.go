package bigfile

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
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
const RefSize = cadata.IDSize + DEKSize

// Ref is a reference to data in a content-addressed store
type Ref struct {
	CID cadata.ID `json:"cid"`
	DEK DEK       `json:"dek"`
}

func RefFromBytes(x []byte) (*Ref, error) {
	if len(x) < RefSize {
		return nil, errors.Errorf("too small to be ref len=%d", len(x))
	}
	r := Ref{}
	br := bytes.NewReader(x)
	for _, slice := range [][]byte{
		r.CID[:],
		r.DEK[:],
	} {
		if _, err := io.ReadFull(br, slice); err != nil {
			return nil, err
		}
	}
	return &r, nil
}

func (r Ref) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, RefSize)
	buf = append(buf, r.CID[:]...)
	buf = append(buf, r.DEK[:]...)
	return buf, nil
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
	ctext := make([]byte, len(ptext))
	dek := encrypt(salt, ctext, ptext)
	cid, err := s.Post(ctx, ctext)
	if err != nil {
		return nil, err
	}
	return &Ref{
		CID: cid,
		DEK: dek,
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
	data := buf[:n]
	o.cache.Add(ref.Key(), data)
	return fn(data)
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
