/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 * Modifications Copyright 2026 drive9 / mem9-ai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"encoding/json"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// memTransport is an in-process JuiceFS-SQL-shaped meta for driver tests.
type memTransport struct {
	mu       sync.Mutex
	counters map[string]int64
	format   []byte
	nodes    map[Ino]*Attr
	edges    map[string]Ino // parent/name -> ino
	chunks   map[string][]byte
	sessions map[uint64][]byte
}

func newMemTransport() *memTransport {
	t := &memTransport{
		counters: map[string]int64{
			"nextInode":   2,
			"nextChunk":   1,
			"nextSession": 0,
		},
		nodes:    map[Ino]*Attr{},
		edges:    map[string]Ino{},
		chunks:   map[string][]byte{},
		sessions: map[uint64][]byte{},
	}
	now := time.Now().Unix()
	t.nodes[RootInode] = &Attr{
		Typ: TypeDirectory, Mode: 0777, Nlink: 2, Length: 4096,
		Parent: RootInode, Full: true, Atime: now, Mtime: now, Ctime: now,
	}
	return t
}

func edgeKey(parent Ino, name string) string {
	return parent.String() + "/" + name
}

func (t *memTransport) Call(ctx Context, op string, req, resp any) syscall.Errno {
	t.mu.Lock()
	defer t.mu.Unlock()
	raw, err := json.Marshal(req)
	if err != nil {
		return syscall.EIO
	}
	switch op {
	case Drive9OpGetCounter:
		var in drive9CounterReq
		_ = json.Unmarshal(raw, &in)
		return encodeResp(resp, &drive9CounterResp{Value: t.counters[in.Name]})
	case Drive9OpIncrCounter:
		var in drive9CounterReq
		_ = json.Unmarshal(raw, &in)
		t.counters[in.Name] += in.Value
		return encodeResp(resp, &drive9CounterResp{Value: t.counters[in.Name]})
	case Drive9OpLoad:
		return encodeResp(resp, &drive9LoadResp{Format: t.format})
	case Drive9OpInit:
		var in drive9InitReq
		_ = json.Unmarshal(raw, &in)
		t.format = in.Format
		return encodeResp(resp, &drive9ErrnoResp{})
	case Drive9OpNewSession, Drive9OpRefreshSession:
		var in drive9SessionReq
		_ = json.Unmarshal(raw, &in)
		t.sessions[in.Sid] = in.Info
		return encodeResp(resp, &drive9ErrnoResp{})
	case Drive9OpLookup:
		var in drive9LookupReq
		_ = json.Unmarshal(raw, &in)
		ino, ok := t.edges[edgeKey(in.Parent, in.Name)]
		if !ok {
			return encodeResp(resp, &drive9NodeResp{Errno: int(syscall.ENOENT)})
		}
		attr := *t.nodes[ino]
		return encodeResp(resp, &drive9NodeResp{Inode: ino, Attr: &attr})
	case Drive9OpGetAttr:
		var in drive9GetAttrReq
		_ = json.Unmarshal(raw, &in)
		attr, ok := t.nodes[in.Inode]
		if !ok {
			return encodeResp(resp, &drive9NodeResp{Errno: int(syscall.ENOENT)})
		}
		cp := *attr
		return encodeResp(resp, &drive9NodeResp{Inode: in.Inode, Attr: &cp})
	case Drive9OpMknod:
		var in drive9MknodReq
		_ = json.Unmarshal(raw, &in)
		if _, ok := t.nodes[in.Parent]; !ok {
			return encodeResp(resp, &drive9NodeResp{Errno: int(syscall.ENOENT)})
		}
		if _, ok := t.edges[edgeKey(in.Parent, in.Name)]; ok {
			return encodeResp(resp, &drive9NodeResp{Errno: int(syscall.EEXIST)})
		}
		attr := in.Attr
		attr.Typ = in.Type
		attr.Mode = in.Mode & ^in.Cumask
		attr.Parent = in.Parent
		attr.Full = true
		if attr.Nlink == 0 {
			if in.Type == TypeDirectory {
				attr.Nlink = 2
			} else {
				attr.Nlink = 1
			}
		}
		t.nodes[in.Inode] = &attr
		t.edges[edgeKey(in.Parent, in.Name)] = in.Inode
		for _, p := range in.Parts {
			key := in.Inode.String() + ":" + itoa(in.Indx)
			buf := marshalSlice(p.Off, p.Slice.Id, p.Slice.Size, p.Slice.Off, p.Slice.Len)
			t.chunks[key] = append(t.chunks[key], buf...)
			newlen := uint64(in.Indx)*ChunkSize + uint64(p.Off) + uint64(p.Slice.Len)
			if newlen > attr.Length {
				attr.Length = newlen
			}
		}
		t.nodes[in.Inode] = &attr
		cp := attr
		return encodeResp(resp, &drive9NodeResp{Inode: in.Inode, Attr: &cp})
	case Drive9OpUnlink:
		var in drive9UnlinkReq
		_ = json.Unmarshal(raw, &in)
		k := edgeKey(in.Parent, in.Name)
		ino, ok := t.edges[k]
		if !ok {
			return encodeResp(resp, &drive9UnlinkResp{Errno: int(syscall.ENOENT)})
		}
		attr := *t.nodes[ino]
		delete(t.edges, k)
		attr.Nlink--
		if attr.Nlink == 0 && !in.Opened {
			delete(t.nodes, ino)
		} else {
			t.nodes[ino] = &attr
		}
		return encodeResp(resp, &drive9UnlinkResp{Attr: &attr})
	case Drive9OpRead:
		var in drive9ReadReq
		_ = json.Unmarshal(raw, &in)
		buf := t.chunks[in.Inode.String()+":"+itoa(in.Indx)]
		if buf == nil {
			buf = []byte{}
		}
		return encodeResp(resp, &drive9ReadResp{Slices: buf})
	case Drive9OpWrite:
		var in drive9WriteReq
		_ = json.Unmarshal(raw, &in)
		attr, ok := t.nodes[in.Inode]
		if !ok {
			return encodeResp(resp, &drive9WriteResp{Errno: int(syscall.ENOENT)})
		}
		key := in.Inode.String() + ":" + itoa(in.Indx)
		parts := in.Parts
		if len(parts) == 0 {
			parts = []WritePart{{Off: in.Off, Slice: in.Slice}}
		}
		var delta int64
		for _, p := range parts {
			buf := marshalSlice(p.Off, p.Slice.Id, p.Slice.Size, p.Slice.Off, p.Slice.Len)
			t.chunks[key] = append(t.chunks[key], buf...)
			newlen := uint64(in.Indx)*ChunkSize + uint64(p.Off) + uint64(p.Slice.Len)
			if newlen > attr.Length {
				delta += int64(newlen - attr.Length)
				attr.Length = newlen
			}
		}
		cp := *attr
		return encodeResp(resp, &drive9WriteResp{
			NumSlices: len(t.chunks[key]) / sliceBytes,
			Attr:      &cp,
			Length:    delta,
			Space:     delta,
		})
	default:
		return encodeResp(resp, &drive9ErrnoResp{})
	}
}

func encodeResp(dst, src any) syscall.Errno {
	b, err := json.Marshal(src)
	if err != nil {
		return syscall.EIO
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return syscall.EIO
	}
	return 0
}

func itoa(v uint32) string {
	return strconv.FormatUint(uint64(v), 10)
}

func TestDrive9MetaCreateLookupUnlink(t *testing.T) {
	tr := newMemTransport()
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	conf.Heartbeat = time.Second
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1})
	ctx = ctx.WithValue(Drive9PathKey, "/foo.db")

	format := &Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20, TrashDays: 0}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := m.Load(false); err != nil {
		t.Fatalf("load: %v", err)
	}

	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "foo.db", 0644, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if ino < 2 {
		t.Fatalf("inode = %d, want >= 2", ino)
	}
	if attr.Typ != TypeFile {
		t.Fatalf("typ = %d, want file", attr.Typ)
	}

	var got Ino
	var gattr Attr
	if st := m.Lookup(ctx, RootInode, "foo.db", &got, &gattr, false); st != 0 {
		t.Fatalf("lookup: %v", st)
	}
	if got != ino {
		t.Fatalf("lookup ino = %d, want %d", got, ino)
	}

	if st := m.Create(ctx, RootInode, "foo.db", 0644, 0, syscall.O_EXCL, &ino, &attr); st != syscall.EEXIST {
		t.Fatalf("exclusive create = %v, want EEXIST", st)
	}

	ctx = ctx.WithValue(Drive9OpenedKey, false)
	if st := m.Unlink(ctx, RootInode, "foo.db"); st != 0 {
		t.Fatalf("unlink: %v", st)
	}
	if st := m.Lookup(ctx, RootInode, "foo.db", &got, &gattr, false); st != syscall.ENOENT {
		t.Fatalf("lookup after unlink = %v, want ENOENT", st)
	}

	var ino2 Ino
	if st := m.Create(ctx, RootInode, "foo.db", 0644, 0, syscall.O_EXCL, &ino2, &attr); st != 0 {
		t.Fatalf("recreate: %v", st)
	}
	if ino2 == ino {
		t.Fatalf("recreate reused inode %d", ino2)
	}
}

type countingTransport struct {
	Drive9Transport
	n map[string]int
}

func (c *countingTransport) Call(ctx Context, op string, req, resp any) syscall.Errno {
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[op]++
	return c.Drive9Transport.Call(ctx, op, req, resp)
}

func TestDrive9UnlinkOpenedKeyOneRPC(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1}).WithValue(Drive9PathKey, "/j.db-journal")
	format := &Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20, TrashDays: 0}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := m.Load(false); err != nil {
		t.Fatalf("load: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "j.db-journal", 0644, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	tr.n = map[string]int{}
	ctx = ctx.WithValue(Drive9OpenedKey, false)
	if st := m.Unlink(ctx, RootInode, "j.db-journal"); st != 0 {
		t.Fatalf("unlink: %v", st)
	}
	if tr.n[Drive9OpLookup] != 0 {
		t.Fatalf("unlink Lookup RPCs=%d, want 0 when Drive9OpenedKey is set (JuiceFS sql IsOpen is same txn)", tr.n[Drive9OpLookup])
	}
	if tr.n[Drive9OpUnlink] != 1 {
		t.Fatalf("unlink HTTP=%d, want 1 (JuiceFS sql_unlink is one txn; Drive9OpenedKey skips extra Lookup)", tr.n[Drive9OpUnlink])
	}
}

func TestDrive9CreateWriteOneHTTP(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	d9 := m.(*drive9Meta)
	ctx := NewContext(1, 1, []uint32{1}).WithValue(Drive9PathKey, "/j.db-journal")
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20, TrashDays: 0}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	tr.n = map[string]int{}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "j.db-journal", 0644, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if tr.n[Drive9OpMknod] != 1 {
		t.Fatalf("create mknod HTTP=%d, want 1 (JuiceFS doMknod is a real insert before Create returns)", tr.n[Drive9OpMknod])
	}
	var got Ino
	if st := m.Lookup(ctx, RootInode, "j.db-journal", &got, &attr, false); st != 0 || got != ino {
		t.Fatalf("lookup: st=%v ino=%d", st, got)
	}
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("newslice: %v", st)
	}
	if st := d9.WriteParts(ctx, ino, 0, []WritePart{{Off: 0, Slice: Slice{Id: id, Size: 4, Len: 4}}}, time.Now()); st != 0 {
		t.Fatalf("write: %v", st)
	}
	if tr.n[Drive9OpMknod] != 1 || tr.n[Drive9OpWrite] != 1 {
		t.Fatalf("create+write mknod=%d write=%d, want 1+1 (JuiceFS create txn then write txn)", tr.n[Drive9OpMknod], tr.n[Drive9OpWrite])
	}
}

func TestDrive9MetaWriteRead(t *testing.T) {
	tr := newMemTransport()
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1})
	_ = m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true)

	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "blob", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := m.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("newslice: %v", st)
	}
	if id == 0 {
		t.Fatalf("slice id is 0")
	}
	sl := Slice{Id: id, Size: 4, Off: 0, Len: 4}
	if st := m.Write(ctx, ino, 0, 0, sl, time.Now()); st != 0 {
		t.Fatalf("write: %v", st)
	}
	var slices []Slice
	if st := m.Read(ctx, ino, 0, &slices); st != 0 {
		t.Fatalf("read: %v", st)
	}
	if len(slices) == 0 {
		t.Fatalf("no slices")
	}
	found := false
	for _, s := range slices {
		if s.Id == id && s.Len == 4 {
			found = true
		}
	}
	if !found {
		t.Fatalf("slices = %+v, missing id=%d", slices, id)
	}
}

func TestDrive9WriteDoesNotInlineCompact(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "blob", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := m.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	tr.n = map[string]int{}
	parts := make([]WritePart, 400)
	for i := range parts {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice: %v", st)
		}
		parts[i] = WritePart{Off: uint32(i * 4), Slice: Slice{Id: id, Size: 4, Len: 4}}
	}
	d9, ok := m.(*drive9Meta)
	if !ok {
		t.Fatal("NewDrive9Meta must return *drive9Meta")
	}
	if st := d9.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	if tr.n[Drive9OpCompact] != 0 {
		t.Fatalf("inline compact RPCs=%d, want 0 (NoBGJob: compact is claim_compact)", tr.n[Drive9OpCompact])
	}
	if tr.n[Drive9OpWrite] != 1 {
		t.Fatalf("WriteParts HTTP write=%d, want 1 (one JuiceFS-shaped write txn)", tr.n[Drive9OpWrite])
	}
}

func TestDrive9ReadDoesNotInlineCompact(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "blob", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := m.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	for i := 0; i < 6; i++ {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice: %v", st)
		}
		sl := Slice{Id: id, Size: 4, Off: 0, Len: 4}
		if st := m.Write(ctx, ino, 0, uint32(i*4), sl, time.Now()); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
	}
	tr.n = map[string]int{}
	var slices []Slice
	if st := m.Read(ctx, ino, 0, &slices); st != 0 {
		t.Fatalf("read: %v", st)
	}
	if len(slices) < 5 {
		t.Fatalf("slices=%d, want ≥5 so JuiceFS SQL Read would compact", len(slices))
	}
	time.Sleep(20 * time.Millisecond)
	if tr.n[Drive9OpCompact] != 0 {
		t.Fatalf("Read compact RPCs=%d, want 0 (NoBGJob: compact is claim_compact)", tr.n[Drive9OpCompact])
	}
}

type delayWriteTransport struct {
	Drive9Transport
	d time.Duration
}

func (d *delayWriteTransport) Call(ctx Context, op string, req, resp any) syscall.Errno {
	if op == Drive9OpWrite || op == Drive9OpMknod {
		time.Sleep(d.d)
	}
	return d.Drive9Transport.Call(ctx, op, req, resp)
}

func TestDrive9FlushDoesNotWaitOtherInodeQueue(t *testing.T) {
	inner := newMemTransport()
	tr := &delayWriteTransport{Drive9Transport: inner, d: 120 * time.Millisecond}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	d9, ok := m.(*drive9Meta)
	if !ok {
		t.Fatal("NewDrive9Meta must return *drive9Meta")
	}
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var a, b Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "a.db", 0644, 0, 0, &a, &attr); st != 0 {
		t.Fatalf("create a: %v", st)
	}
	if st := m.Create(ctx, RootInode, "b.db", 0644, 0, 0, &b, &attr); st != 0 {
		t.Fatalf("create b: %v", st)
	}
	if st := m.Open(ctx, a, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open a: %v", st)
	}
	if st := m.Open(ctx, b, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open b: %v", st)
	}
	var ida, idb uint64
	if st := m.NewSlice(ctx, &ida); st != 0 {
		t.Fatalf("newslice a: %v", st)
	}
	if st := m.NewSlice(ctx, &idb); st != 0 {
		t.Fatalf("newslice b: %v", st)
	}
	if st := d9.QueueWriteParts(ctx, a, 0, []WritePart{{Off: 0, Slice: Slice{Id: ida, Size: 4, Len: 4}}}, time.Now()); st != 0 {
		t.Fatalf("queue a: %v", st)
	}
	start := time.Now()
	if st := d9.WriteParts(ctx, b, 0, []WritePart{{Off: 0, Slice: Slice{Id: idb, Size: 4, Len: 4}}}, time.Now()); st != 0 {
		t.Fatalf("flush b: %v", st)
	}
	elapsed := time.Since(start)
	if elapsed >= 200*time.Millisecond {
		t.Fatalf("Flush inode B waited %s; global FIFO serializes inodes (JuiceFS commitThread is per file)", elapsed)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("write HTTP delay not applied: %s", elapsed)
	}
}

func TestDrive9WaitWritesDrainsSameInodeQueue(t *testing.T) {
	inner := newMemTransport()
	tr := &delayWriteTransport{Drive9Transport: inner, d: 120 * time.Millisecond}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	d9 := m.(*drive9Meta)
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "a.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := m.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("newslice: %v", st)
	}
	if st := d9.QueueWriteParts(ctx, ino, 0, []WritePart{{Off: 0, Slice: Slice{Id: id, Size: 4, Len: 4}}}, time.Now()); st != 0 {
		t.Fatalf("queue: %v", st)
	}
	start := time.Now()
	if st := d9.WaitWrites(ino); st != 0 {
		t.Fatalf("WaitWrites: %v", st)
	}
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond {
		t.Fatalf("WaitWrites returned in %s, want to drain the 120ms write", elapsed)
	}
}

func TestDrive9QueueWritePartsBatchesHTTP(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	d9, ok := m.(*drive9Meta)
	if !ok {
		t.Fatal("NewDrive9Meta must return *drive9Meta")
	}
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "blob", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := m.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	tr.n = map[string]int{}
	for i := 0; i < 16; i++ {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice: %v", st)
		}
		parts := []WritePart{{Off: uint32(i * 4), Slice: Slice{Id: id, Size: 4, Len: 4}}}
		if st := d9.QueueWriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
			t.Fatalf("queue %d: %v", i, st)
		}
	}
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("newslice: %v", st)
	}
	if st := d9.WriteParts(ctx, ino, 0, []WritePart{{Off: 64, Slice: Slice{Id: id, Size: 4, Len: 4}}}, time.Now()); st != 0 {
		t.Fatalf("flush write: %v", st)
	}
	httpN := tr.n[Drive9OpWrite] + tr.n[Drive9OpMknod]
	if httpN == 0 {
		t.Fatal("expected at least one HTTP write after WriteParts wait")
	}
	if httpN >= 16 {
		t.Fatalf("HTTP writes=%d, want <16 (queued slices batched)", httpN)
	}
}

func TestDrive9WritePartsCompactsAtMaxSlices(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "fat.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := m.(interface {
		WriteParts(Context, Ino, uint32, []WritePart, time.Time) syscall.Errno
	})
	parts := make([]WritePart, maxSlices)
	for i := range parts {
		parts[i] = WritePart{Off: uint32(i), Slice: Slice{Id: uint64(i + 1), Size: 1, Len: 1}}
	}
	tr.n = map[string]int{}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	deadline := time.After(2 * time.Second)
	for tr.n[Drive9OpRead] == 0 {
		select {
		case <-deadline:
			t.Fatal("numSlices=maxSlices must launch compactChunk (doRead)")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestDrive9WritePartsDoesNotCompactBelowMaxSlices(t *testing.T) {
	inner := newMemTransport()
	tr := &countingTransport{Drive9Transport: inner, n: map[string]int{}}
	conf := DefaultConf()
	conf.NoBGJob = true
	conf.MaxDeletes = 0
	m := NewDrive9Meta(conf, tr)
	ctx := NewContext(1, 1, []uint32{1})
	if err := m.Init(&Format{Name: "drive9", UUID: "test", Storage: "file", BlockSize: 4 << 20}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "thin.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := m.(interface {
		WriteParts(Context, Ino, uint32, []WritePart, time.Time) syscall.Errno
	})
	parts := make([]WritePart, 400)
	for i := range parts {
		parts[i] = WritePart{Off: uint32(i), Slice: Slice{Id: uint64(i + 1), Size: 1, Len: 1}}
	}
	tr.n = map[string]int{}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	time.Sleep(50 * time.Millisecond)
	if tr.n[Drive9OpRead] != 0 {
		t.Fatalf("numSlices=400 launched compactChunk reads=%d (HTTP compact only at maxSlices)", tr.n[Drive9OpRead])
	}
}


