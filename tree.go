package glfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state/cadata"
)

// Tree is a directory of entries to other trees or blobs
// Each entry in a tree has a unique name.
type Tree struct {
	Entries []TreeEntry
}

// Lookup returns the entry in the tree with name if it exists, or nil if it does not.
func (t *Tree) Lookup(name string) *TreeEntry {
	i := sort.Search(len(t.Entries), func(i int) bool {
		return t.Entries[i].Name >= name
	})
	if i >= 0 && i < len(t.Entries) && t.Entries[i].Name == name {
		return &t.Entries[i]
	}
	return nil
}

func (t *Tree) MarshalText() ([]byte, error) {
	sort.SliceStable(t.Entries, t.sorter)
	if err := t.Validate(); err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	for _, ent := range t.Entries {
		data, err := json.Marshal(ent)
		if err != nil {
			return nil, err
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

func (t *Tree) UnmarshalText(x []byte) error {
	entries := []TreeEntry{}
	lines := bytes.Split(x, []byte("\n"))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		ent := TreeEntry{}
		if err := json.Unmarshal(line, &ent); err != nil {
			err = fmt.Errorf("unmarshaling tree: %w", err)
			return err
		}
		entries = append(entries, ent)
	}
	t.Entries = entries
	if err := t.Validate(); err != nil {
		t.Entries = nil
		return err
	}
	return nil
}

func (t *Tree) Validate() error {
	if !sort.SliceIsSorted(t.Entries, t.sorter) {
		return errors.New("tree entries are not sorted")
	}
	if err := t.checkDuplicates(); err != nil {
		return err
	}
	for _, ent := range t.Entries {
		if err := ent.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Replace replaces the entry in tree with name == ent.Name with ent.
func (t *Tree) Replace(ent TreeEntry) {
	for i := range t.Entries {
		if t.Entries[i].Name == ent.Name {
			t.Entries[i] = ent
		}
	}
}

func (t *Tree) sorter(i, j int) bool {
	return t.Entries[i].Name < t.Entries[j].Name
}

func (t *Tree) checkDuplicates() error {
	for i := 0; i < len(t.Entries)-1; i++ {
		if t.Entries[i].Name == t.Entries[i+1].Name {
			return fmt.Errorf("duplicate tree entry %v %v", t.Entries[i], t.Entries[i+1])
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

	t, err := ag.GetTree(ctx, store, ref)
	if err != nil {
		return nil, err
	}
	ent := t.Lookup(parts[0])
	if ent == nil {
		return nil, ErrNoEnt{Name: parts[0]}
	}
	return ag.GetAtPath(ctx, store, ent.Ref, parts[1])
}

// PostTree writes a tree to CA storage and returns a Ref pointing to it.
func (ag *Agent) PostTree(ctx context.Context, store cadata.Poster, t Tree) (*Ref, error) {
	tw := ag.NewTreeWriter(store)
	sort.SliceStable(t.Entries, t.sorter)
	for _, ent := range t.Entries {
		if err := tw.Put(ctx, ent); err != nil {
			return nil, err
		}
	}
	return tw.Finish(ctx)
}

// GetTree retreives the tree in store at Ref if it exists.
// If ref.Type != TypeTree ErrRefType is returned.
func (ag *Agent) GetTree(ctx context.Context, store cadata.Getter, ref Ref) (*Tree, error) {
	tr, err := ag.NewTreeReader(store, ref)
	if err != nil {
		return nil, err
	}
	ents, err := streams.Collect(ctx, tr, 1<<32)
	if err != nil {
		return nil, err
	}
	return &Tree{Entries: ents}, nil
}

func readTree(r io.Reader) (*Tree, error) {
	tree := &Tree{}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err := tree.UnmarshalText(data); err != nil {
		return nil, err
	}
	return tree, nil
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
	tree, err := ag.GetTree(ctx, store, ref)
	if err != nil {
		return err
	}
	for _, ent := range tree.Entries {
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
		tree, err := ag.GetTree(ctx, s, ref)
		if err != nil {
			return err
		}
		for _, ent := range tree.Entries {
			if err := ag.WalkRefs(ctx, s, ent.Ref, fn); err != nil {
				return err
			}
		}
	}
	return fn(ref)
}

func (ag *Agent) PostTreeEntries(ctx context.Context, s cadata.Poster, ents []TreeEntry) (*Ref, error) {
	tree := Tree{}
	subents := map[string][]TreeEntry{}
	for _, ent := range ents {
		p := CleanPath(ent.Name)
		if p == "" {
			return &ent.Ref, nil
		}
		parts := strings.SplitN(p, "/", 2)
		if len(parts) == 1 {
			tree.Entries = append(tree.Entries, TreeEntry{
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
		ref, err := ag.PostTreeEntries(ctx, s, ents2)
		if err != nil {
			return nil, err
		}
		tree.Entries = append(tree.Entries, TreeEntry{
			Name:     k,
			FileMode: getFileMode(*ref),
			Ref:      *ref,
		})
	}
	return ag.PostTree(ctx, s, tree)
}

func (ag *Agent) PostTreeMap(ctx context.Context, s cadata.Poster, m map[string]Ref) (*Ref, error) {
	entries := []TreeEntry{}
	for k, v := range m {
		entries = append(entries, TreeEntry{
			Name:     k,
			FileMode: getFileMode(v),
			Ref:      v,
		})
	}
	return ag.PostTreeEntries(ctx, s, entries)
}

func getFileMode(tr Ref) os.FileMode {
	if tr.Type == TypeTree {
		return 0755 | os.ModeDir
	}
	return 0644
}

func CleanPath(x string) string {
	x = path.Clean(x)
	x = strings.Trim(x, "/")
	if x == "." {
		return ""
	}
	return x
}

func IsValidName(x string) bool {
	return x != "" && !strings.Contains(x, "/")
}

type TreeWriter struct {
	tw       *TypedWriter
	enc      *json.Encoder
	lastName string
}

func (ag *Agent) NewTreeWriter(s cadata.Poster) *TreeWriter {
	tw := ag.NewTypedWriter(s, TypeTree)
	return &TreeWriter{
		tw:  tw,
		enc: json.NewEncoder(tw),
	}
}

func (tw *TreeWriter) Put(ctx context.Context, te TreeEntry) error {
	if te.Name <= tw.lastName {
		return fmt.Errorf("cannot write tree entries out of order %q <= %q", te.Name, tw.lastName)
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

	dec  *json.Decoder
	last string
}

func (ag *Agent) NewTreeReader(s cadata.Getter, x Ref) (*TreeReader, error) {
	if x.Type != TypeTree {
		return nil, ErrRefType{Have: x.Type, Want: TypeTree}
	}
	return &TreeReader{ag: ag, s: s, ref: x}, nil
}

func (tr *TreeReader) Next(ctx context.Context, dst *TreeEntry) error {
	if tr.dec == nil {
		r, err := tr.ag.GetTyped(ctx, tr.s, TypeTree, tr.ref)
		if err != nil {
			return err
		}
		tr.dec = json.NewDecoder(r)
	}
	if !tr.dec.More() {
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
