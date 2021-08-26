package cache

import "context"

//Transaction in batches, groups of segment
func SegmentLoop(ctx context.Context, arrayLength, segment int, handler func(start, end int) error) error {
	groups := arrayLength / segment
	rest := arrayLength % segment

	for i := 0; i < groups; i++ {
		err := handler(i*segment, (i+1)*segment)
		if err != nil {
			return err
		}
	}
	if rest > 0 {
		err := handler(groups*segment, groups*segment+rest)
		if err != nil {
			return err
		}
	}
	return nil
}
