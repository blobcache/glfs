package glfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path"
	"slices"
	"sort"
	"strings"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
)

// Lookup returns the entry in the tree with name if it exists, or nil if it does not.
func Lookup(ents []TreeEntry, name string) *TreeEntry {
	i := sort.Search(len(ents), func(i int) bool {
		return ents[i].Name >= name
	})
	if i >= 0 && i < len(ents) && ents[i].Name == name {
		return &ents[i]
	}
	return nil
}

func SortTreeEntries(ents []TreeEntry) {
	slices.SortFunc(ents, compareTreeEnt)
}

func ValidateTreeEntries(ents []TreeEntry) error {
	if !slices.IsSortedFunc(ents, compareTreeEnt) {
		return errors.New("tree entries are not sorted")
	}
	if err := checkDuplicates(ents); err != nil {
		return err
	}
	for _, ent := range ents {
		if err := ent.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Replace replaces the entry in tree with name == ent.Name with ent.
func Replace(ents []TreeEntry, ent TreeEntry) {
	for i := range ents {
		if ents[i].Name == ent.Name {
			ents[i] = ent
		}
	}
}

func compareTreeEnt(a, b TreeEntry) int {
	return strings.Compare(a.Name, b.Name)
}

func checkDuplicates(ents []TreeEntry) error {
	for i := 0; i < len(ents)-1; i++ {
		if ents[i].Name == ents[i+1].Name {
			return fmt.Errorf("duplicate tree entry %v %v", ents[i], ents[i+1])
		}
	}
	return nil
}

// TreeEntry is a single entry in a tree, uniquely identified by Name
type TreeEntry struct {
	Name     string      `json:"name"`
	FileMode os.FileMode `json:"mode"`
	Ref      Ref         `json:"ref"`
}

func (te *TreeEntry) Validate() error {
	cleaned := CleanPath(te.Name)
	if cleaned != te.Name {
		return fmt.Errorf("name (%s) is not properly cleaned", te.Name)
	}
	if te.Name == "" {
		return errors.New("TreeEntry name cannot be empty")
	}
	return nil
}

// GetAtPath returns a ref to the object under ref at subpath.
// ErrNoEnt is returned if there is no entry at that path.
func (ag *Agent) GetAtPath(ctx context.Context, store cadata.Getter, ref Ref, subpath string) (*Ref, error) {
	subpath = strings.Trim(subpath, "/")
	if subpath == "" {
		return &ref, nil
	}
	if ref.Type != TypeTree {
		return nil, errors.New("can only take subpath of type tree")
	}

	parts := strings.SplitN(subpath, "/", 2)
	if len(parts) < 2 {
		parts = append(parts, "")
	}
	t, err := ag.NewTreeReader(store, ref)
	if err != nil {
		return nil, err
	}
	for {
		ent, err := streams.Next(ctx, t)
		if err != nil {
			if streams.IsEOS(err) {
				break
			}
			return nil, err
		}
		if ent.Name == parts[0] {
			return ag.GetAtPath(ctx, store, ent.Ref, parts[1])
		} else if ent.Name > parts[0] {
			break
		}
	}
	return nil, ErrNoEnt{Name: parts[0]}
}

// GetTree retreives the tree in store at Ref if it exists.
// If ref.Type != TypeTree ErrRefType is returned.
func (ag *Agent) GetTreeSlice(ctx context.Context, store cadata.Getter, ref Ref, maxEnts int) ([]TreeEntry, error) {
	tr, err := ag.NewTreeReader(store, ref)
	if err != nil {
		return nil, err
	}
	return streams.Collect(ctx, tr, maxEnts)
}

// WalkTreeFunc is the type of functions passed to WalkTree
type WalkTreeFunc = func(prefix string, tree TreeEntry) error

// WalkTree walks the tree and calls f with tree entries in lexigraphical order
// file1.txt comes before file2.txt
// dir1/ comes before dir1/file1.txt
func (ag *Agent) WalkTree(ctx context.Context, store cadata.Getter, ref Ref, f WalkTreeFunc) error {
	return ag.walkTree(ctx, store, ref, f, "")
}

func (ag *Agent) walkTree(ctx context.Context, store cadata.Getter, ref Ref, f WalkTreeFunc, prefix string) error {
	// TODO: use TreeReader
	ents, err := ag.GetTreeSlice(ctx, store, ref, 1e6)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		if err := f(prefix, ent); err != nil {
			return err
		}
		if ent.Ref.Type == TypeTree {
			p2 := path.Join(prefix, ent.Name)
			if err := ag.walkTree(ctx, store, ent.Ref, f, p2); err != nil {
				return err
			}
		}
	}
	return nil
}

type RefWalker func(ref Ref) error

// WalkRefs calls fn with every Ref reacheable from ref, including Ref. The only guarentee about order is bottom up.
// if a tree is encoutered the child refs will be visited first.
func (ag *Agent) WalkRefs(ctx context.Context, s cadata.Getter, ref Ref, fn RefWalker) error {
	if ref.Type == TypeTree {
		// TODO: use tree reader
		ents, err := ag.GetTreeSlice(ctx, s, ref, 1e6)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := ag.WalkRefs(ctx, s, ent.Ref, fn); err != nil {
				return err
			}
		}
	}
	return fn(ref)
}

func (ag *Agent) PostTree(ctx context.Context, s cadata.PostExister, ents iter.Seq[TreeEntry]) (*Ref, error) {
	var rootEnts []TreeEntry
	subents := map[string][]TreeEntry{}
	for ent := range ents {
		p := CleanPath(ent.Name)
		if p == "" {
			return &ent.Ref, nil
		}
		parts := strings.SplitN(p, "/", 2)
		if len(parts) == 1 {
			rootEnts = append(rootEnts, TreeEntry{
				Name:     parts[0],
				FileMode: ent.FileMode,
				Ref:      ent.Ref,
			})
		} else {
			subents[parts[0]] = append(subents[parts[0]], TreeEntry{
				Name:     parts[1],
				FileMode: ent.FileMode,
				Ref:      ent.Ref,
			})
		}
	}

	for k, ents2 := range subents {
		ref, err := ag.PostTreeSlice(ctx, s, ents2)
		if err != nil {
			return nil, err
		}
		rootEnts = append(rootEnts, TreeEntry{
			Name:     k,
			FileMode: getFileMode(*ref),
			Ref:      *ref,
		})
	}
	SortTreeEntries(rootEnts)
	tw := ag.NewTreeWriter(s)
	for _, ent := range rootEnts {
		if err := tw.Put(ctx, ent); err != nil {
			return nil, err
		}
	}
	return tw.Finish(ctx)
}

func (ag *Agent) PostTreeSlice(ctx context.Context, dst cadata.PostExister, ents []TreeEntry) (*Ref, error) {
	return ag.PostTree(ctx, dst, func(yield func(TreeEntry) bool) {
		for _, ent := range ents {
			if !yield(ent) {
				return
			}
		}
	})
}

func (ag *Agent) PostTreeMap(ctx context.Context, s cadata.PostExister, m map[string]Ref) (*Ref, error) {
	entries := []TreeEntry{}
	for k, v := range m {
		entries = append(entries, TreeEntry{
			Name:     k,
			FileMode: getFileMode(v),
			Ref:      v,
		})
	}
	return ag.PostTreeSlice(ctx, s, entries)
}

func getFileMode(tr Ref) os.FileMode {
	if tr.Type == TypeTree {
		return 0755 | os.ModeDir
	}
	return 0644
}

// CleanPath removes leading and trailing slashes, and changes "." to ""
func CleanPath(x string) string {
	x = path.Clean(x)
	x = strings.Trim(x, "/")
	if x == "." {
		return ""
	}
	return x
}

// IsValidName returns true if x can be used as a TreeEntry name
func IsValidName(x string) bool {
	return x != "" && !strings.Contains(x, "/")
}

type TreeWriter struct {
	dst      cadata.PostExister
	tw       *TypedWriter
	enc      *json.Encoder
	lastName string
}

func (ag *Agent) NewTreeWriter(s cadata.PostExister) *TreeWriter {
	tw := ag.NewTypedWriter(s, TypeTree)
	return &TreeWriter{
		dst: s,
		tw:  tw,
		enc: json.NewEncoder(tw),
	}
}

func (tw *TreeWriter) Put(ctx context.Context, te TreeEntry) error {
	if te.Name <= tw.lastName {
		return fmt.Errorf("cannot write tree entries out of order %q <= %q", te.Name, tw.lastName)
	}
	if yes, err := tw.dst.Exists(ctx, te.Ref.CID); err != nil {
		return err
	} else if !yes {
		return fmt.Errorf("adding tree ent %v would violate referential integrity", te)
	}
	tw.tw.SetWriteContext(ctx)
	defer tw.tw.SetWriteContext(nil)
	if err := tw.enc.Encode(te); err != nil {
		return err
	}
	tw.lastName = te.Name
	return nil
}

func (tw *TreeWriter) Finish(ctx context.Context) (*Ref, error) {
	return tw.tw.Finish(ctx)
}

type TreeReader struct {
	ag  *Agent
	s   cadata.Getter
	ref Ref

	r    io.Reader
	dec  *json.Decoder
	last string
}

func (ag *Agent) NewTreeReader(s cadata.Getter, x Ref) (*TreeReader, error) {
	if x.Type != TypeTree {
		return nil, ErrRefType{Have: x.Type, Want: TypeTree}
	}
	return &TreeReader{ag: ag, s: s, ref: x}, nil
}

func (ag *Agent) ReadTreeFrom(r io.Reader) *TreeReader {
	return &TreeReader{ag: ag, r: r}
}

func (tr *TreeReader) Next(ctx context.Context, dst *TreeEntry) error {
	if tr.dec == nil {
		r, err := tr.ag.GetTyped(ctx, tr.s, TypeTree, tr.ref)
		if err != nil {
			return err
		}
		tr.r = r
		tr.dec = json.NewDecoder(r)
	}

	if !tr.dec.More() {
		if _, err := tr.r.Read(nil); !errors.Is(err, io.EOF) {
			return err
		}
		return streams.EOS()
	}
	if err := tr.dec.Decode(dst); err != nil {
		return err
	}
	if dst.Name <= tr.last {
		return fmt.Errorf("tree entries are out of order: %v <= %v", dst.Name, tr.last)
	}
	if err := dst.Validate(); err != nil {
		return err
	}
	tr.last = dst.Name
	return nil
}
