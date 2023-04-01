package slices2

import (
	"context"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func ParMapErr[A, B any, SA ~[]A](ctx context.Context, sem *semaphore.Weighted, as SA, fn func(context.Context, A) (B, error)) ([]B, error) {
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	eg, ctx := errgroup.WithContext(ctx)
	bs := make([]B, len(as))
	for i := range as {
		i := i
		if ok := sem.TryAcquire(1); ok {
			eg.Go(func() (err error) {
				defer sem.Release(1)
				bs[i], err = fn(ctx, as[i])
				return err
			})
		} else {
			var err error
			bs[i], err = fn(ctx, as[i])
			if err != nil {
				cf()
				eg.Wait()
				return nil, err
			}
		}
	}
	return bs, eg.Wait()
}

func ParForEach[A any, SA ~[]A](ctx context.Context, sem *semaphore.Weighted, as SA, fn func(context.Context, A) error) error {
	_, err := ParMapErr(ctx, sem, as, func(ctx context.Context, x A) (struct{}, error) {
		err := fn(ctx, x)
		return struct{}{}, err
	})
	return err
}
