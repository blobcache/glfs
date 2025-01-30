package glfsposix

import (
	"context"
	"fmt"
	"io"
	"path"

	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/posixfs"
	"golang.org/x/sync/semaphore"

	"github.com/blobcache/glfs"
	"github.com/blobcache/glfs/internal/slices2"
)

// Import goes from a POSIX filesystem to GLFS
func Import(ctx context.Context, ag *glfs.Agent, sem *semaphore.Weighted, s cadata.Poster, fsx posixfs.FS, p string) (*glfs.Ref, error) {
	return glfsImport(ctx, glfsImportParams{
		ag:  ag,
		sem: sem,
		s:   s,

		fs:     fsx,
		target: p,
	})
}

type glfsImportParams struct {
	ag  *glfs.Agent
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
				ag:     p.ag,
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
		return p.ag.PostTreeSlice(ctx, p.s, tents)
	}
	// regular file
	f, err := p.fs.OpenFile(p.target, posixfs.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return p.ag.PostBlob(ctx, p.s, f)
}

// Export exports a glfs object beneath p in the filesystem fsx.
func Export(ctx context.Context, ag *glfs.Agent, sem *semaphore.Weighted, s cadata.Getter, root glfs.Ref, fsx posixfs.FS, p string) error {
	fileMode := posixfs.FileMode(0o644)
	if root.Type == glfs.TypeTree {
		fileMode = 0o755
	}
	return glfsExport(ctx, glfsExportParams{
		ag:       ag,
		sem:      sem,
		s:        s,
		fs:       fsx,
		ref:      root,
		target:   p,
		fileMode: fileMode,
	})
}

type glfsExportParams struct {
	ag       *glfs.Agent
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
		// TODO: use TreeReader
		tree, err := p.ag.GetTreeSlice(ctx, p.s, p.ref, 1e6)
		if err != nil {
			return err
		}
		return slices2.ParForEach(ctx, p.sem, tree, func(ctx context.Context, x glfs.TreeEntry) error {
			p2 := p
			p2.target = path.Join(p.target, x.Name)
			p2.ref = x.Ref
			// p2.fileMode = x.FileMode
			p2.fileMode = 0o644
			if p2.ref.Type == glfs.TypeTree {
				p2.fileMode = 0o755
			}
			return glfsExport(ctx, p2)
		})
	case glfs.TypeBlob:
		f, err := p.fs.OpenFile(p.target, posixfs.O_CREATE|posixfs.O_EXCL|posixfs.O_WRONLY, p.fileMode)
		if err != nil {
			return err
		}
		defer f.Close()
		r, err := p.ag.GetBlob(ctx, p.s, p.ref)
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
