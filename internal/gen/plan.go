package gen

import (
	"errors"
	"fmt"
)

// PlanConfig sizes work units.
type PlanConfig struct {
	UnitRecords  int64
	UnitMaxBytes int64
}

// UnitSpec is one schedulable range of a stream: indices [Start, End).
type UnitSpec struct {
	Stream     StreamID
	Seq        uint64
	Start, End uint64
}

// typicalRecordFraction estimates a typical record from its worst case: sizing units by
// the worst case alone would make them needlessly small.
const typicalRecordFraction = 4

// unitWidth returns how many indices one unit covers. Sparse streams need more indices
// per row, and a unit never splits one parent's run of indices.
func unitWidth(s StreamSpec, cfg PlanConfig) uint64 {
	rows := cfg.UnitRecords
	if s.MaxRecordBytes > 0 && cfg.UnitMaxBytes > 0 {
		typical := max(s.MaxRecordBytes/typicalRecordFraction, 1)
		rows = min(rows, max(cfg.UnitMaxBytes/typical, 1))
	}
	width := uint64(max(rows, 1))
	if s.ExpectedRows > 0 && s.Space > 0 {
		if density := s.ExpectedRows / float64(s.Space); density > 0 && density < 1 {
			width = uint64(float64(width)/density) + 1
		}
	}
	if per := max(s.PerParent, 1); per > 1 {
		if width%per != 0 {
			width += per - width%per
		}
	}
	return width
}

// PlanBounded splits streams with a known size into units covering each exactly once.
// count, if not zero, is the size of streams whose Space is 0.
func PlanBounded(streams []StreamSpec, count uint64, cfg PlanConfig) ([]UnitSpec, error) {
	var units []UnitSpec
	for _, s := range streams {
		total := s.Space
		if total == 0 {
			total = count
		}
		if total == 0 {
			return nil, fmt.Errorf("stream %s has no size: a record count is required", s.ID)
		}
		width := unitWidth(s, cfg)
		var seq uint64
		for start := uint64(0); start < total; seq++ {
			end := min(start+width, total)
			if end <= start { // overflow near the top of the index space
				end = total
			}
			units = append(units, UnitSpec{Stream: s.ID, Seq: seq, Start: start, End: end})
			start = end
		}
	}
	return units, nil
}

// Cursor is where lazy planning of a stream continues.
type Cursor struct {
	Seq  uint64
	Next uint64
}

// ErrStreamEnd means a lazily planned stream has no more units.
var ErrStreamEnd = errors.New("stream has no more units")

// NextUnit creates the next unit of a stream, for sessions planned as they run. limit is
// the end of the index space, or 0 for none. Relational streams always have one: a
// streaming session over tables paces their declared rows and then ends.
func NextUnit(s StreamSpec, cfg PlanConfig, cur Cursor, limit uint64) (UnitSpec, Cursor, error) {
	end := s.Space
	if limit != 0 && (end == 0 || limit < end) {
		end = limit
	}
	if end != 0 && cur.Next >= end {
		return UnitSpec{}, cur, ErrStreamEnd
	}
	stop := cur.Next + unitWidth(s, cfg)
	if stop < cur.Next { // the 64-bit index space is exhausted
		return UnitSpec{}, cur, ErrStreamEnd
	}
	if end != 0 {
		stop = min(stop, end)
	}
	unit := UnitSpec{Stream: s.ID, Seq: cur.Seq, Start: cur.Next, End: stop}
	return unit, Cursor{Seq: cur.Seq + 1, Next: stop}, nil
}
