package glfsposix

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"golang.org/x/sync/semaphore"

	"github.com/blobcache/glfs"
	"github.com/blobcache/glfs/internal/slices2"
)

// Import goes from a POSIX filesystem to GLFS
func Import(ctx context.Context, op *glfs.Operator, sem *semaphore.Weighted, s cadata.Poster, fsx posixfs.FS, p string) (*glfs.Ref, error) {
	return glfsImport(ctx, glfsImportParams{
		op:  op,
		sem: sem,
		s:   s,

		fs:     fsx,
		target: p,
	})
}

type glfsImportParams struct {
	op  *glfs.Operator
	sem *semaphore.Weighted
	s   cadata.Poster

	fs     posixfs.FS
	target string
}

func glfsImport(ctx context.Context, p glfsImportParams) (*glfs.Ref, error) {
	finfo, err := p.fs.Stat(p.target)
	if err != nil {
		return nil, err
	}
	// dir
	if finfo.IsDir() {
		ents, err := posixfs.ReadDir(p.fs, p.target)
		if err != nil {
			return nil, err
		}
		tents, err := slices2.ParMapErr(ctx, p.sem, ents, func(ctx context.Context, ent posixfs.DirEnt) (glfs.TreeEntry, error) {
			p2 := path.Join(p.target, ent.Name)
			ref2, err := glfsImport(ctx, glfsImportParams{
				op:     p.op,
				s:      p.s,
				sem:    p.sem,
				fs:     p.fs,
				target: p2,
			})
			if err != nil {
				return glfs.TreeEntry{}, err
			}
			return glfs.TreeEntry{
				Name:     ent.Name,
				FileMode: ent.Mode,
				Ref:      *ref2,
			}, nil
		})
		if err != nil {
			return nil, err
		}
		return p.op.PostTreeFromEntries(ctx, p.s, tents)
	}
	// regular file
	f, err := p.fs.OpenFile(p.target, posixfs.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return p.op.PostBlob(ctx, p.s, f)
}

// Export exports a glfs object beneath p in the filesystem fsx.
func Export(ctx context.Context, op *glfs.Operator, sem *semaphore.Weighted, s cadata.Getter, root glfs.Ref, fsx posixfs.FS, p string) error {
	fileMode := posixfs.FileMode(0o644)
	if root.Type == glfs.TypeTree {
		fileMode = 0o755
	}
	return glfsExport(ctx, glfsExportParams{
		op:       op,
		sem:      sem,
		s:        s,
		fs:       fsx,
		ref:      root,
		target:   p,
		fileMode: fileMode,
	})
}

type glfsExportParams struct {
	op       *glfs.Operator
	s        cadata.Getter
	sem      *semaphore.Weighted
	fs       posixfs.FS
	ref      glfs.Ref
	target   string
	fileMode posixfs.FileMode
}

func glfsExport(ctx context.Context, p glfsExportParams) error {
	switch p.ref.Type {
	case glfs.TypeTree:
		if err := p.fs.Mkdir(p.target, p.fileMode); err != nil {
			return err
		}
		tree, err := p.op.GetTree(ctx, p.s, p.ref)
		if err != nil {
			return err
		}
		return slices2.ParForEach(ctx, p.sem, tree.Entries, func(ctx context.Context, x glfs.TreeEntry) error {
			p2 := p
			p2.target = path.Join(p.target, x.Name)
			return glfsExport(ctx, p2)
		})
	case glfs.TypeBlob:
		f, err := p.fs.OpenFile(p.target, posixfs.O_CREATE|posixfs.O_EXCL, p.fileMode)
		if err != nil {
			return err
		}
		defer f.Close()
		r, err := p.op.GetBlob(ctx, p.s, p.ref)
		if err != nil {
			return err
		}
		if _, err = io.Copy(f, r); err != nil {
			return err
		}
		return f.Close()
	default:
		return fmt.Errorf("unrecognzied type %q", p.ref.Type)
	}
}
