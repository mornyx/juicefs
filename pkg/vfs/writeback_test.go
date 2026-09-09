package vfs

import (
	"bytes"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

func TestWriteBackUsesRemainingHandleAfterRelease(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	fe, fh1, e := v.Create(ctx, 1, "shm.bin", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	_, fh2, e := v.Open(ctx, fe.Inode, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("open: %s", e)
	}
	payload := bytes.Repeat([]byte{0xA5}, 4096)
	if e = v.Write(ctx, fe.Inode, payload, 0, fh1); e != 0 {
		t.Fatalf("write fh1: %s", e)
	}
	v.Release(ctx, fe.Inode, fh1)
	more := bytes.Repeat([]byte{0x5A}, 4096)
	if e = v.WriteBack(ctx, fe.Inode, more, 0); e != 0 {
		t.Fatalf("WriteBack after Release: %s", e)
	}
	got := make([]byte, 4096)
	n, e := v.Read(ctx, fe.Inode, got, 0, fh2)
	if e != 0 || n != 4096 {
		t.Fatalf("read fh2 n=%d err=%s", n, e)
	}
	if !bytes.Equal(got, more) {
		t.Fatalf("WriteBack data not visible on remaining handle")
	}
}
