package vfs

import (
	"syscall"
	"testing"
)

func TestLeadingDoneSlicesStopsAtFirstPending(t *testing.T) {
	a := &sliceWriter{done: true}
	b := &sliceWriter{done: true}
	c := &sliceWriter{done: false}
	if n := leadingDoneSlices([]*sliceWriter{a, b, c}); n != 2 {
		t.Fatalf("leadingDoneSlices=%d, want 2", n)
	}
	d := &sliceWriter{done: true, err: syscall.EIO}
	if n := leadingDoneSlices([]*sliceWriter{a, d}); n != 1 {
		t.Fatalf("error slice must not be in the WriteParts prefix, got %d", n)
	}
	if n := leadingDoneSlices(nil); n != 0 {
		t.Fatalf("nil=%d", n)
	}
}

func TestWritePartsBatchCeiling(t *testing.T) {
	if writePartsBatch < 2 {
		t.Fatal("WriteParts batch must coalesce more than one slice")
	}
}

func TestLeadingDoneSlicesCapsAtReadyPrefix(t *testing.T) {
	done := make([]*sliceWriter, 8)
	for i := range done {
		done[i] = &sliceWriter{done: true}
	}
	if n := leadingDoneSlices(done); n != 8 {
		t.Fatalf("all done: %d", n)
	}
}

func TestFrozenInflightCountsFreezedNotDone(t *testing.T) {
	a := &sliceWriter{done: true, freezed: true}
	b := &sliceWriter{done: false, freezed: true}
	c := &sliceWriter{done: false, freezed: false}
	if n := frozenInflight([]*sliceWriter{a, b, c}); n != 1 {
		t.Fatalf("frozenInflight=%d, want 1 (flushData in flight)", n)
	}
	if n := frozenInflight([]*sliceWriter{a}); n != 0 {
		t.Fatalf("done slice is not in-flight: %d", n)
	}
}

func TestClampCommitCount(t *testing.T) {
	if clampCommitCount(0) != 1 {
		t.Fatal("empty prefix still commits the waiting slice")
	}
	if clampCommitCount(writePartsBatch+8) != writePartsBatch {
		t.Fatal("batch ceiling")
	}
}
