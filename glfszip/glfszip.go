package glfszip

import (
	"archive/zip"
	"context"
	"io"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
)

// Import creates a glfs.Tree from the contents of a zip.Reader: zr.
func Import(ctx context.Context, ag *glfs.Agent, s cadata.Poster, zr *zip.Reader) (*glfs.Ref, error) {
	var ents []glfs.TreeEntry
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		if err := func() error {
			defer rc.Close()
			w := ag.NewBlobWriter(ctx, s)
			if _, err := io.Copy(w, rc); err != nil {
				return err
			}
			ref, err := w.Finish(ctx)
			if err != nil {
				return err
			}
			ents = append(ents, glfs.TreeEntry{
				Name:     f.Name,
				FileMode: f.FileInfo().Mode(),
				Ref:      *ref,
			})
			return nil
		}(); err != nil {
			return nil, err
		}
	}
	return ag.PostTreeEntries(ctx, s, ents)
}
