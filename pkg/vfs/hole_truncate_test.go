package vfs

import (
	"bytes"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Kernel writeback_cache + ftruncate: pwrite-into-hole stays in page cache
// until setattr returns, then writepages, then invalidate. Userspace sees
// Truncate first, then Write, then Read (fsx seed 1790593139).
func TestHoleWriteAfterTruncateIsVisible(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	fe, fh, e := v.Create(ctx, 1, "hole.bin", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	head := bytes.Repeat([]byte{0xAA}, 145032)
	if e = v.Write(ctx, fe.Inode, head, 0, fh); e != 0 {
		t.Fatalf("write head: %s", e)
	}
	if e = v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
		t.Fatalf("flush head: %s", e)
	}
	var attr meta.Attr
	if e = v.Truncate(ctx, fe.Inode, 246680, fh, &attr); e != 0 {
		t.Fatalf("truncate: %s", e)
	}
	pattern := bytes.Repeat([]byte{0x5d, 0x01, 0x00, 0x00}, 63268/4)
	if e = v.Write(ctx, fe.Inode, pattern, 196504, fh); e != 0 {
		t.Fatalf("writeback hole write: %s", e)
	}
	got := make([]byte, 32)
	n, e := v.Read(ctx, fe.Inode, got, 197572, fh)
	if e != 0 {
		t.Fatalf("read: %s", e)
	}
	if n != 32 {
		t.Fatalf("short read n=%d", n)
	}
	want := pattern[197572-196504 : 197572-196504+32]
	if !bytes.Equal(got, want) {
		t.Fatalf("after truncate-then-writeback got %x want %x", got, want)
	}
}

func TestHoleWriteBeforeTruncateIsVisible(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	fe, fh, e := v.Create(ctx, 1, "hole2.bin", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	head := bytes.Repeat([]byte{0xAA}, 145032)
	if e = v.Write(ctx, fe.Inode, head, 0, fh); e != 0 {
		t.Fatalf("write head: %s", e)
	}
	pattern := bytes.Repeat([]byte{0x5d, 0x01, 0x00, 0x00}, 63268/4)
	if e = v.Write(ctx, fe.Inode, pattern, 196504, fh); e != 0 {
		t.Fatalf("hole write: %s", e)
	}
	var attr meta.Attr
	if e = v.Truncate(ctx, fe.Inode, 246680, fh, &attr); e != 0 {
		t.Fatalf("truncate: %s", e)
	}
	got := make([]byte, 32)
	n, e := v.Read(ctx, fe.Inode, got, 197572, fh)
	if e != 0 {
		t.Fatalf("read: %s", e)
	}
	if n != 32 {
		t.Fatalf("short read n=%d", n)
	}
	want := pattern[197572-196504 : 197572-196504+32]
	if !bytes.Equal(got, want) {
		t.Fatalf("after write-then-truncate got %x want %x", got, want)
	}
}
