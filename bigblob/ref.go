package bigblob

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
)

const DEKSize = 32

// DEK is a Data Encryption Key
type DEK [DEKSize]byte

func (dek DEK) String() string {
	h := blake3.Sum256(dek[:])
	return fmt.Sprintf("{DEK %x}", h[:4])
}

func (dek DEK) MarshalJSON() (ret []byte, _ error) {
	ret = append(ret, '"')
	ret = hex.AppendEncode(ret, dek[:])
	ret = append(ret, '"')
	return ret, nil
}

func (dek *DEK) UnmarshalJSON(data []byte) error {
	var hexStr string
	if err := json.Unmarshal(data, &hexStr); err != nil {
		return err
	}
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return err
	}
	if len(data) != len(dek) {
		return fmt.Errorf("DEK.UnmarshalJSON: data is wrong length %d", len(data))
	}
	copy(dek[:], data)
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
		return nil, fmt.Errorf("too small to be ref len=%d", len(x))
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
	buf, _ := r.MarshalBinary()
	return blake3.Sum256(buf[:])
}

func (r1 Ref) Equals(r2 Ref) bool {
	return hmac.Equal(r1.CID[:], r2.CID[:]) && hmac.Equal(r1.DEK[:], r2.DEK[:])
}

func marshalRef(x Ref) []byte {
	data, _ := x.MarshalBinary()
	return data
}

func (ag *Agent) post(ctx context.Context, s cadata.Poster, salt *[32]byte, ptext []byte) (*Ref, error) {
	buf := ag.acquireBuffer(len(ptext))
	defer ag.releaseBuffer(buf)
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

func (ag *Agent) getF(ctx context.Context, s cadata.Getter, ref Ref, fn func([]byte) error) error {
	if value, ok := ag.cache.Get(ref.Key()); ok {
		return fn(value)
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
	ag.cache.Add(ref.Key(), data)
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
