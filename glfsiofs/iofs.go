package glfsiofs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"go.brendoncarroll.net/exp/streams"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
)

var _ fs.FS = &FS{}

type FS struct {
	ag   *glfs.Machine
	s    schema.RO
	root glfs.Ref
}

func New(s schema.RO, root glfs.Ref) *FS {
	return &FS{
		ag:   glfs.NewMachine(),
		s:    s,
		root: root,
	}
}

func (s *FS) Open(p string) (fs.File, error) {
	ctx := context.TODO()
	if !fs.ValidPath(p) {
		return nil, &fs.PathError{Path: p, Op: "open"}
	}
	p = strings.Trim(p, "/")
	if p == "." {
		p = ""
	}
	ent, err := s.ag.Lookup(ctx, s.s, glfs.TreeEntry{Ref: s.root}, p)
	if err != nil {
		if glfs.IsErrNoEnt(err) {
			err = fs.ErrNotExist
		}
		return nil, err
	}
	return newGLFSFile(ctx, s.ag, s.s, *ent), nil
}

var _ fs.File = &File{}

type File struct {
	ctx context.Context
	ag  *glfs.Machine
	s   schema.RO

	ref  glfs.Ref
	name string
	mode os.FileMode

	r  *glfs.Reader
	tr *glfs.TreeReader
}

func newGLFSFile(ctx context.Context, ag *glfs.Machine, s schema.RO, ent glfs.TreeEntry) *File {
	return &File{
		ctx: ctx,
		ag:  ag,
		s:   s,

		name: ent.Name,
		mode: ent.FileMode,
		ref:  ent.Ref,
	}
}

func (f *File) Read(buf []byte) (int, error) {
	if f.ref.Type == glfs.TypeTree {
		return 0, fs.ErrInvalid
	}
	ctx := f.ctx
	if f.r == nil {
		r, err := f.ag.GetBlob(ctx, f.s, f.ref)
		if err != nil {
			return 0, err
		}
		f.r = r
	}
	n, err := f.r.Read(buf)
	return n, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.r.Seek(offset, whence)
}

func (f *File) Stat() (fs.FileInfo, error) {
	finfo := &fileInfo{
		name: f.name,
		mode: f.mode,
		size: int64(f.ref.Size),
	}
	if f.ref.Type == glfs.TypeTree {
		finfo.mode |= fs.ModeDir
	}
	return finfo, nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) ReadDir(n int) ([]fs.DirEntry, error) {
	ctx := context.TODO()
	if f.ref.Type != glfs.TypeTree {
		return nil, errors.New("read on non-Tree")
	}
	if f.tr == nil {
		tr, err := f.ag.NewTreeReader(f.s, f.ref)
		if err != nil {
			return nil, err
		}
		f.tr = tr
	}
	var ret []fs.DirEntry
	if n <= 0 {
		if err := streams.ForEach(ctx, f.tr, func(ent glfs.TreeEntry) error {
			ret = append(ret, dirEnt{
				name: ent.Name,
				mode: ent.FileMode,
				size: int64(ent.Ref.Size),
			})
			return nil
		}); err != nil {
			return nil, err
		}
		return ret, nil
	}
	for i := 0; i < n; i++ {
		ent, err := streams.Next(ctx, f.tr)
		if err != nil {
			if streams.IsEOS(err) {
				return ret, io.EOF
			}
			return nil, err
		}
		ret = append(ret, dirEnt{
			name: ent.Name,
			mode: ent.FileMode,
			size: int64(ent.Ref.Size),
		})
	}
	return ret, nil
}

type fileInfo struct {
	name    string
	mode    fs.FileMode
	modTime time.Time
	size    int64
}

func (fi fileInfo) Name() string {
	return fi.name
}

func (fi fileInfo) Mode() fs.FileMode {
	return fi.mode
}

func (fi fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi fileInfo) IsDir() bool {
	return fi.mode.IsDir()
}

func (fi fileInfo) Sys() any {
	return nil
}

func (fi fileInfo) Size() int64 {
	return fi.size
}

type dirEnt struct {
	name string
	mode fs.FileMode
	size int64
}

func (de dirEnt) Name() string {
	return de.name
}

func (de dirEnt) Info() (fs.FileInfo, error) {
	return fileInfo{
		name: de.name,
		mode: de.mode,
		size: de.size,
	}, nil
}

func (dr dirEnt) IsDir() bool {
	return dr.mode.IsDir()
}

func (dr dirEnt) Type() fs.FileMode {
	return dr.mode & fs.ModeType
}
