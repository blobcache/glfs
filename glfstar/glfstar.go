// package glfstar converts GLFS to/from TAR
package glfstar

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
)

const MaxPathLen = 4096

// WriteTAR writes the GLFS filesystem at root to tw.
func WriteTAR(ctx context.Context, ag *glfs.Machine, s schema.RO, root glfs.Ref, tw *tar.Writer) error {
	if root.Type == glfs.TypeBlob {
		r, err := ag.GetBlob(ctx, s, root)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: "",
			Mode: 0644,
			Size: int64(glfs.SizeOf(root)),
		}); err != nil {
			return err
		}
		if _, err := io.Copy(tw, r); err != nil {
			return err
		}
		return tw.Close()
	}
	err := ag.WalkTree(ctx, s, root, func(prefix string, ent glfs.TreeEntry) error {
		p := path.Join(prefix, ent.Name)
		mode := ent.FileMode
		switch ent.Ref.Type {
		case glfs.TypeBlob:
			th := &tar.Header{
				Name: p,
				Mode: int64(mode),
			}
			switch {
			case os.FileMode(mode)&os.ModeSymlink > 0:
				data, err := ag.GetBlobBytes(ctx, s, ent.Ref, MaxPathLen)
				if err != nil {
					return err
				}
				th.Typeflag = tar.TypeSymlink
				th.Linkname = string(data)
			default:
				th.Typeflag = tar.TypeReg
				th.Size = int64(ent.Ref.Size)
			}
			if err := tw.WriteHeader(th); err != nil {
				return err
			}
			if th.Size > 0 {
				r, err := ag.GetBlob(ctx, s, ent.Ref)
				if err != nil {
					return err
				}
				if _, err := io.Copy(tw, r); err != nil {
					return err
				}
			}
		case glfs.TypeTree:
			th := &tar.Header{
				Name: p + "/",
				Mode: int64(mode),
			}
			if err := tw.WriteHeader(th); err != nil {
				return err
			}
		default:
			return fmt.Errorf("cannot write type %s to tar stream", ent.Ref.Type)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ReadTAR creates a GLFS filesystem with contents read from tr
func ReadTAR(ctx context.Context, ag *glfs.Machine, s schema.WO, tr *tar.Reader) (*glfs.Ref, error) {
	ents := []glfs.TreeEntry{}
	emptyDirs := map[string]glfs.TreeEntry{}
	for {
		th, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		name := clean(th.Name)
		if name == "" {
			continue
		}
		mode := th.Mode
		var ref *glfs.Ref
		switch th.Typeflag {
		case tar.TypeDir:
			mode |= int64(os.ModeDir)
			ref, err := ag.PostTreeSlice(ctx, s, []glfs.TreeEntry{})
			if err != nil {
				return nil, err
			}
			emptyDirs[name] = glfs.TreeEntry{
				Name:     name,
				FileMode: os.FileMode(mode),
				Ref:      *ref,
			}
			delete(emptyDirs, parentOf(name))
			continue
		case tar.TypeSymlink, tar.TypeLink:
			mode |= int64(os.ModeSymlink)
			ref, err = ag.PostBlob(ctx, s, strings.NewReader(th.Linkname))
			if err != nil {
				return nil, err
			}
		default:
			ref, err = ag.PostBlob(ctx, s, tr)
			if err != nil {
				return nil, err
			}
		}
		ent := glfs.TreeEntry{
			Name:     name,
			FileMode: os.FileMode(mode),
			Ref:      *ref,
		}
		ents = append(ents, ent)
		delete(emptyDirs, parentOf(name))
	}
	for _, ent := range emptyDirs {
		ents = append(ents, ent)
	}
	return ag.PostTreeSlice(ctx, s, ents)
}

func clean(x string) string {
	return glfs.CleanPath(x)
}

func parentOf(x string) string {
	const sep = "/"
	x = strings.Trim(x, sep)
	parts := strings.Split(x, sep)
	if len(parts) > 0 {
		parts = parts[:len(parts)-1]
	}
	return clean(strings.Join(parts, sep))
}
