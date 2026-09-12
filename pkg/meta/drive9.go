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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Drive9PathKey is the meta.Context value carrying the drive9 projection path
// for Create/Unlink/Mkdir/Rename. The HTTP engine sends it so the server can
// dual-write JuiceFS node/edge and file_nodes in one transaction.
const Drive9PathKey CtxKey = "drive9.path"

// Drive9OpenedKey is set by the FUSE frontend when the unlinking client still
// holds the file open. The server then inserts a sustained row instead of
// dropping the JuiceFS node (open-unlinked).
const Drive9OpenedKey CtxKey = "drive9.opened"

// Drive9Transport is the stateless RPC used by the drive9 meta engine.
// Implementations live in the drive9 client; this package stays free of HTTP.
type Drive9Transport interface {
	Call(ctx Context, op string, req, resp any) syscall.Errno
}

// Drive9 op names. The drive9-server /v1/extent/meta handler must match these.
const (
	Drive9OpGetCounter        = "get_counter"
	Drive9OpIncrCounter       = "incr_counter"
	Drive9OpSetIfSmall        = "set_if_small"
	Drive9OpLoad              = "load"
	Drive9OpInit              = "init"
	Drive9OpNewSession        = "new_session"
	Drive9OpRefreshSession    = "refresh_session"
	Drive9OpFindStaleSessions = "find_stale_sessions"
	Drive9OpCleanStaleSession = "clean_stale_session"
	Drive9OpLookup            = "lookup"
	Drive9OpGetAttr           = "getattr"
	Drive9OpSetAttr           = "setattr"
	Drive9OpMknod             = "mknod"
	Drive9OpUnlink            = "unlink"
	Drive9OpRmdir             = "rmdir"
	Drive9OpRename            = "rename"
	Drive9OpReaddir           = "readdir"
	Drive9OpRead              = "read"
	Drive9OpWrite             = "write"
	Drive9OpTruncate          = "truncate"
	Drive9OpCompact           = "compact"
	Drive9OpDeleteSlice       = "delete_slice"
	Drive9OpDeleteSustained   = "delete_sustained"
	Drive9OpFlock             = "flock"
	Drive9OpGetlk             = "getlk"
	Drive9OpSetlk             = "setlk"
	Drive9OpGetSession        = "get_session"
)

// ErrCompactDelegated is returned from OnMsg(CompactChunk) so baseMeta.Write
// does not CAS-commit a slice the client never uploaded. Compaction is
// server-scheduled and client-executed via Drive9CommitCompact.
var ErrCompactDelegated = fmt.Errorf("compact delegated to drive9 server")

type drive9WriteJob struct {
	inode Ino
	indx  uint32
	parts []WritePart
	mtime time.Time
	done  chan syscall.Errno
}

// drive9InodeWriter is JuiceFS fileWriter.commitThread: one serial Meta.Write
// stream per inode. A global FIFO made Flush of file B wait for file A's HTTP
// (speedtest delete 12s→26s). Different inodes run in parallel like SQL.
type drive9InodeWriter struct {
	q chan drive9WriteJob
	// pending counts enqueued jobs whose commit has not reached metadata yet.
	// A slice that already left the writer's buffer is only readable once its
	// metadata commit lands, so readers consult this before serving a read
	// from the write buffer instead of flushing.
	pending atomic.Int64
	// seq increments on every enqueue. A reader records it around its read so
	// that a commit which came and went during the read still invalidates the
	// reader window that read may have cached from the pre-commit mapping.
	seq atomic.Uint64
	// err is the sticky first commit failure of this inode. Queued commits
	// (QueueWriteParts, done == nil) have nobody to hand their error to, so it
	// is remembered here and reported by the next WaitWrites — otherwise a
	// Flush could return success although the batch it waited behind failed
	// and the data it acknowledged is not referenced by metadata. It stays set
	// for the inode's writer lifetime, matching fileWriter.err upstream.
	err atomic.Int32
}

// recordCommitError remembers the first commit failure of an inode.
func (w *drive9InodeWriter) recordCommitError(st syscall.Errno) {
	if w == nil || st == 0 {
		return
	}
	w.err.CompareAndSwap(0, int32(st))
}

// takeCommitError reports the sticky commit failure, if any. The error is
// sticky (not consumed): the bytes it refers to are still missing from
// metadata, so every later barrier for this inode must keep failing.
func (w *drive9InodeWriter) takeCommitError() syscall.Errno {
	if w == nil {
		return 0
	}
	if st := w.err.Load(); st != 0 {
		return syscall.Errno(st)
	}
	return 0
}

// drive9ChunkCacheTTL bounds how long a chunk mapping cached by this client is
// trusted without asking the server again. The mount is close-to-open for
// everything else, so a mapping only has to outlive the read-your-writes
// pattern it was cached for; after this it is re-read inside a transaction.
const drive9ChunkCacheTTL = time.Second

type drive9Meta struct {
	*baseMeta
	tr      Drive9Transport
	writeMu sync.Mutex
	writers map[Ino]*drive9InodeWriter

	chunkAtMu sync.Mutex
	chunkAt   map[Ino]map[uint32]time.Time
}

// noteChunkCached records when a chunk mapping was last confirmed by the server.
func (m *drive9Meta) noteChunkCached(inode Ino, indx uint32) {
	m.chunkAtMu.Lock()
	defer m.chunkAtMu.Unlock()
	if m.chunkAt == nil || len(m.chunkAt) > 8192 {
		// Bounded: a size limit only costs refetches, never correctness.
		m.chunkAt = make(map[Ino]map[uint32]time.Time)
	}
	byIndx := m.chunkAt[inode]
	if byIndx == nil {
		byIndx = make(map[uint32]time.Time)
		m.chunkAt[inode] = byIndx
	}
	byIndx[indx] = time.Now()
}

// chunkCacheFresh reports whether a cached mapping is still within its TTL.
func (m *drive9Meta) chunkCacheFresh(inode Ino, indx uint32) bool {
	m.chunkAtMu.Lock()
	defer m.chunkAtMu.Unlock()
	byIndx := m.chunkAt[inode]
	if byIndx == nil {
		return false
	}
	at, ok := byIndx[indx]
	return ok && time.Since(at) < drive9ChunkCacheTTL
}

var _ Meta = (*drive9Meta)(nil)
var _ engine = (*drive9Meta)(nil)

// NewDrive9Meta constructs a Meta client whose engine methods are RPCs.
// conf.NoBGJob should be true: block GC and compaction are server-side.
func NewDrive9Meta(conf *Config, tr Drive9Transport) Meta {
	if conf == nil {
		conf = DefaultConf()
	} else {
		conf.SelfCheck()
	}
	if conf.MaxDeletes == 0 {
		conf.MaxDeletes = 0
	}
	m := &drive9Meta{
		tr:      tr,
		writers: make(map[Ino]*drive9InodeWriter),
	}
	m.baseMeta = newBaseMeta("drive9", conf)
	m.en = m
	return m
}

func init() {
	Register("drive9", func(driver, addr string, conf *Config) (Meta, error) {
		return nil, fmt.Errorf("drive9 meta must be constructed with meta.NewDrive9Meta")
	})
}

func drive9Path(ctx Context) string {
	if ctx == nil {
		return ""
	}
	v := ctx.Value(Drive9PathKey)
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func drive9Opened(ctx Context) (opened bool, set bool) {
	if ctx == nil {
		return false, false
	}
	v := ctx.Value(Drive9OpenedKey)
	if v == nil {
		return false, false
	}
	b, _ := v.(bool)
	return b, true
}

func (m *drive9Meta) call(ctx Context, op string, req, resp any) syscall.Errno {
	if ctx == nil {
		ctx = Background()
	}
	if m.tr == nil {
		return syscall.EIO
	}
	return m.tr.Call(ctx, op, req, resp)
}

func (m *drive9Meta) Name() string { return "drive9" }

func (m *drive9Meta) Shutdown() error {
	if m.of != nil {
		m.of.close()
	}
	return nil
}

func (m *drive9Meta) Reset() error { return fmt.Errorf("drive9 meta reset is not supported") }

func (m *drive9Meta) ListSessions() ([]*Session, error) { return nil, nil }

func (m *drive9Meta) ListLocks(_ context.Context, _ Ino) ([]PLockItem, []FLockItem, error) {
	return nil, nil, nil
}

func (m *drive9Meta) GetXattr(_ Context, _ Ino, _ string, _ *[]byte) syscall.Errno {
	return syscall.ENOTSUP
}

func (m *drive9Meta) ListXattr(_ Context, _ Ino, _ *[]byte) syscall.Errno { return syscall.ENOTSUP }

func (m *drive9Meta) CopyFileRange(_ Context, _ Ino, _ uint64, _ Ino, _ uint64, _ uint64, _ uint32, copied, outLength *uint64) syscall.Errno {
	if copied != nil {
		*copied = 0
	}
	if outLength != nil {
		*outLength = 0
	}
	return syscall.ENOTSUP
}

func (m *drive9Meta) ListSlices(_ Context, _ map[Ino][]Slice, _, _ bool, _ func()) syscall.Errno {
	return 0
}

func (m *drive9Meta) DumpMeta(_ io.Writer, _ Ino, _ int, _, _, _ bool) error {
	return fmt.Errorf("drive9 meta dump is not supported")
}

func (m *drive9Meta) LoadMeta(_ io.Reader) error {
	return fmt.Errorf("drive9 meta load is not supported")
}

func (m *drive9Meta) ScanChangelog(_ Context, _ int64, _ func(ver int64, entry string) error) error {
	return nil
}

func (m *drive9Meta) Flock(ctx Context, inode Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	for {
		var resp drive9ErrnoResp
		st := m.call(ctx, Drive9OpFlock, &drive9FlockReq{
			Inode: inode,
			Sid:   m.sid,
			Owner: owner,
			Ltype: ltype,
		}, &resp)
		if st != 0 {
			return st
		}
		if resp.Errno == 0 {
			return 0
		}
		eno := syscall.Errno(resp.Errno)
		if eno != syscall.EAGAIN || !block {
			return eno
		}
		if ctx != nil && ctx.Canceled() {
			return syscall.EINTR
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (m *drive9Meta) Getlk(ctx Context, inode Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	var resp drive9GetlkResp
	st := m.call(ctx, Drive9OpGetlk, &drive9GetlkReq{
		Inode: inode,
		Sid:   m.sid,
		Owner: owner,
		Ltype: *ltype,
		Start: *start,
		End:   *end,
		Pid:   *pid,
	}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	*ltype = resp.Ltype
	*start = resp.Start
	*end = resp.End
	*pid = resp.Pid
	return 0
}

func (m *drive9Meta) Setlk(ctx Context, inode Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	for {
		var resp drive9ErrnoResp
		st := m.call(ctx, Drive9OpSetlk, &drive9SetlkReq{
			Inode: inode,
			Sid:   m.sid,
			Owner: owner,
			Ltype: ltype,
			Start: start,
			End:   end,
			Pid:   pid,
		}, &resp)
		if st != 0 {
			return st
		}
		if resp.Errno == 0 {
			return 0
		}
		eno := syscall.Errno(resp.Errno)
		if eno != syscall.EAGAIN || !block {
			return eno
		}
		if ctx != nil && ctx.Canceled() {
			return syscall.EINTR
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// Drive9CommitCompact CAS-commits a compacted slice. Used by the drive9
// compact executor after it has uploaded the new block.
func Drive9CommitCompact(m Meta, inode Ino, indx uint32, origin []byte, skipped int, pos uint32, id uint64, size uint32) syscall.Errno {
	d, ok := m.(*drive9Meta)
	if !ok {
		return syscall.ENOTSUP
	}
	return d.doCompactChunk(inode, indx, origin, nil, skipped, pos, id, size, nil)
}

type drive9ErrnoResp struct {
	Errno int `json:"errno"`
}

type drive9CounterReq struct {
	Name  string `json:"name"`
	Value int64  `json:"value,omitempty"`
	Diff  int64  `json:"diff,omitempty"`
}

type drive9CounterResp struct {
	Errno int   `json:"errno"`
	Value int64 `json:"value"`
	OK    bool  `json:"ok,omitempty"`
}

type drive9LoadResp struct {
	Errno  int    `json:"errno"`
	Format []byte `json:"format"`
}

type drive9InitReq struct {
	Format json.RawMessage `json:"format"`
	Force  bool            `json:"force"`
}

type drive9SessionReq struct {
	Sid    uint64 `json:"sid"`
	Expire int64  `json:"expire"`
	Info   []byte `json:"info"`
	Update bool   `json:"update"`
}

type drive9FindStaleReq struct {
	Limit int `json:"limit"`
}

type drive9FindStaleResp struct {
	Errno int      `json:"errno"`
	Sids  []uint64 `json:"sids"`
}

type drive9LookupReq struct {
	Parent Ino    `json:"parent"`
	Name   string `json:"name"`
}

type drive9NodeResp struct {
	Errno int   `json:"errno"`
	Inode Ino   `json:"inode"`
	Attr  *Attr `json:"attr,omitempty"`
}

type drive9GetAttrReq struct {
	Inode Ino `json:"inode"`
}

type drive9SetAttrReq struct {
	Inode          Ino    `json:"inode"`
	Set            uint16 `json:"set"`
	SugidClearMode uint8  `json:"sugid_clear_mode"`
	Attr           Attr   `json:"attr"`
}

type drive9MknodReq struct {
	Parent   Ino         `json:"parent"`
	Name     string      `json:"name"`
	Type     uint8       `json:"type"`
	Mode     uint16      `json:"mode"`
	Cumask   uint16      `json:"cumask"`
	Path     string      `json:"path,omitempty"`
	Inode    Ino         `json:"inode"`
	Attr     Attr        `json:"attr"`
	ProjPath string      `json:"proj_path,omitempty"`
	Uid      uint32      `json:"uid"`
	Gid      uint32      `json:"gid"`
	Indx     uint32      `json:"indx,omitempty"`
	Parts    []WritePart `json:"parts,omitempty"`
	Mtime    time.Time   `json:"mtime,omitempty"`
}

type drive9UnlinkReq struct {
	Parent    Ino    `json:"parent"`
	Name      string `json:"name"`
	SkipTrash bool   `json:"skip_trash"`
	ProjPath  string `json:"proj_path,omitempty"`
	Sid       uint64 `json:"sid"`
	Opened    bool   `json:"opened"`
}

type drive9UnlinkResp struct {
	Errno int   `json:"errno"`
	Attr  *Attr `json:"attr,omitempty"`
}

type drive9RmdirReq struct {
	Parent    Ino    `json:"parent"`
	Name      string `json:"name"`
	SkipTrash bool   `json:"skip_trash"`
	ProjPath  string `json:"proj_path,omitempty"`
}

type drive9RenameReq struct {
	SrcParent Ino    `json:"src_parent"`
	SrcName   string `json:"src_name"`
	DstParent Ino    `json:"dst_parent"`
	DstName   string `json:"dst_name"`
	Flags     uint32 `json:"flags"`
	SrcPath   string `json:"src_path,omitempty"`
	DstPath   string `json:"dst_path,omitempty"`
}

type drive9RenameResp struct {
	Errno  int   `json:"errno"`
	Inode  Ino   `json:"inode"`
	Tinode Ino   `json:"tinode"`
	Attr   *Attr `json:"attr,omitempty"`
	Tattr  *Attr `json:"tattr,omitempty"`
}

type drive9ReaddirReq struct {
	Inode Ino   `json:"inode"`
	Plus  uint8 `json:"plus"`
	Limit int   `json:"limit"`
}

type drive9ReaddirResp struct {
	Errno   int      `json:"errno"`
	Entries []*Entry `json:"entries"`
}

type drive9ReadReq struct {
	Inode Ino    `json:"inode"`
	Indx  uint32 `json:"indx"`
}

type drive9ReadResp struct {
	Errno  int    `json:"errno"`
	Slices []byte `json:"slices"`
}

// WritePart is one slice in a Flush-time batch (one HTTP, one TiDB txn).
type WritePart struct {
	Off   uint32 `json:"off"`
	Slice Slice  `json:"slice"`
}

type drive9WriteReq struct {
	Inode Ino         `json:"inode"`
	Indx  uint32      `json:"indx"`
	Off   uint32      `json:"off"`
	Slice Slice       `json:"slice"`
	Parts []WritePart `json:"parts,omitempty"`
	Mtime time.Time   `json:"mtime"`
}

type drive9WriteResp struct {
	Errno     int   `json:"errno"`
	NumSlices int   `json:"num_slices"`
	Attr      *Attr `json:"attr,omitempty"`
	Length    int64 `json:"delta_length"`
	Space     int64 `json:"delta_space"`
	// Slices is the chunk's mapping after this write, as the server computed it
	// inside the same transaction. Caching it keeps the open-file chunk cache
	// valid across our own writes: one chunk spans 64 MB, so dropping the
	// mapping would cost an HTTP metadata query on every following read.
	Slices []byte `json:"slices,omitempty"`
}

type drive9TruncateReq struct {
	Inode         Ino    `json:"inode"`
	Flags         uint8  `json:"flags"`
	Length        uint64 `json:"length"`
	SkipPermCheck bool   `json:"skip_perm_check"`
}

type drive9TruncateResp struct {
	Errno  int   `json:"errno"`
	Attr   *Attr `json:"attr,omitempty"`
	Length int64 `json:"delta_length"`
	Space  int64 `json:"delta_space"`
}

type drive9CompactReq struct {
	Inode   Ino    `json:"inode"`
	Indx    uint32 `json:"indx"`
	Origin  []byte `json:"origin"`
	Skipped int    `json:"skipped"`
	Pos     uint32 `json:"pos"`
	Id      uint64 `json:"id"`
	Size    uint32 `json:"size"`
	Delayed []byte `json:"delayed,omitempty"`
}

type drive9DeleteSliceReq struct {
	Id   uint64 `json:"id"`
	Size uint32 `json:"size"`
}

type drive9SustainedReq struct {
	Sid   uint64 `json:"sid"`
	Inode Ino    `json:"inode"`
}

type drive9FlockReq struct {
	Inode Ino    `json:"inode"`
	Sid   uint64 `json:"sid"`
	Owner uint64 `json:"owner"`
	Ltype uint32 `json:"ltype"`
}

type drive9GetlkReq struct {
	Inode Ino    `json:"inode"`
	Sid   uint64 `json:"sid"`
	Owner uint64 `json:"owner"`
	Ltype uint32 `json:"ltype"`
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	Pid   uint32 `json:"pid"`
}

type drive9GetlkResp struct {
	Errno int    `json:"errno"`
	Ltype uint32 `json:"ltype"`
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	Pid   uint32 `json:"pid"`
}

type drive9SetlkReq struct {
	Inode Ino    `json:"inode"`
	Sid   uint64 `json:"sid"`
	Owner uint64 `json:"owner"`
	Ltype uint32 `json:"ltype"`
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	Pid   uint32 `json:"pid"`
}

type drive9GetSessionReq struct {
	Sid    uint64 `json:"sid"`
	Detail bool   `json:"detail"`
}

type drive9GetSessionResp struct {
	Errno   int      `json:"errno"`
	Session *Session `json:"session,omitempty"`
}

type staticDirHandler struct {
	mu      sync.Mutex
	entries []*Entry
}

func (h *staticDirHandler) List(_ Context, offset int) ([]*Entry, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if offset >= len(h.entries) {
		return nil, 0
	}
	return h.entries[offset:], 0
}

func (h *staticDirHandler) Insert(inode Ino, name string, attr *Attr) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, &Entry{Inode: inode, Name: []byte(name), Attr: attr})
}

func (h *staticDirHandler) Delete(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := []byte(name)
	out := h.entries[:0]
	for _, e := range h.entries {
		if !bytes.Equal(e.Name, n) {
			out = append(out, e)
		}
	}
	h.entries = out
}

func (h *staticDirHandler) Read(int) {}
func (h *staticDirHandler) Close()   {}

func respErrno(errno int, st syscall.Errno) syscall.Errno {
	if st != 0 {
		return st
	}
	if errno == 0 {
		return 0
	}
	return syscall.Errno(errno)
}

func copyAttr(dst *Attr, src *Attr) {
	if dst == nil || src == nil {
		return
	}
	*dst = *src
}
