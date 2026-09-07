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
		buf := marshalSlice(in.Off, in.Slice.Id, in.Slice.Size, in.Slice.Off, in.Slice.Len)
		t.chunks[key] = append(t.chunks[key], buf...)
		newlen := uint64(in.Indx)*ChunkSize + uint64(in.Off) + uint64(in.Slice.Len)
		delta := int64(0)
		if newlen > attr.Length {
			delta = int64(newlen - attr.Length)
			attr.Length = newlen
		}
		cp := *attr
		return encodeResp(resp, &drive9WriteResp{
			NumSlices: len(t.chunks[key]) / sliceBytes,
			Attr:      &cp,
			Length:    delta,
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
