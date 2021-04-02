package sync

type DiffType int

const (
	DataDiffType 	DiffType = iota
	MatchDiffType
)

type DataDiff struct {
	Offset		int
	Size		int
}

type MatchDiff struct {
	SourceOffset		int
	DestinationOffset	int
}

type Diff struct {
	Type		DiffType
	DataDiff	DataDiff
	MatchDiff	MatchDiff
}
