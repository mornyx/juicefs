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
	"syscall"
	"time"

	aclAPI "github.com/juicedata/juicefs/pkg/acl"
	"github.com/juicedata/juicefs/pkg/utils"
	"google.golang.org/protobuf/proto"
)

func (m *drive9Meta) getCounter(name string) (int64, error) {
	var resp drive9CounterResp
	st := m.call(Background(), Drive9OpGetCounter, &drive9CounterReq{Name: name}, &resp)
	if st != 0 {
		return 0, st
	}
	if resp.Errno != 0 {
		return 0, syscall.Errno(resp.Errno)
	}
	return resp.Value, nil
}

func (m *drive9Meta) incrCounter(name string, value int64) (int64, error) {
	var resp drive9CounterResp
	st := m.call(Background(), Drive9OpIncrCounter, &drive9CounterReq{Name: name, Value: value}, &resp)
	if st != 0 {
		return 0, st
	}
	if resp.Errno != 0 {
		return 0, syscall.Errno(resp.Errno)
	}
	return resp.Value, nil
}

func (m *drive9Meta) setIfSmall(name string, value, diff int64) (bool, error) {
	var resp drive9CounterResp
	st := m.call(Background(), Drive9OpSetIfSmall, &drive9CounterReq{Name: name, Value: value, Diff: diff}, &resp)
	if st != 0 {
		return false, st
	}
	if resp.Errno != 0 {
		return false, syscall.Errno(resp.Errno)
	}
	return resp.OK, nil
}

func (m *drive9Meta) updateStats(int64, int64) {}
func (m *drive9Meta) doFlushStats()            {}

func (m *drive9Meta) doLoad() ([]byte, error) {
	var resp drive9LoadResp
	st := m.call(Background(), Drive9OpLoad, struct{}{}, &resp)
	if st != 0 {
		return nil, st
	}
	if resp.Errno != 0 {
		return nil, syscall.Errno(resp.Errno)
	}
	return resp.Format, nil
}

func (m *drive9Meta) doInit(format *Format, force bool) error {
	raw, err := json.Marshal(format)
	if err != nil {
		return err
	}
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpInit, &drive9InitReq{Format: raw, Force: force}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	m.Lock()
	m.fmt = format
	m.Unlock()
	return nil
}

func (m *drive9Meta) doNewSession(sinfo []byte, update bool) error {
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpNewSession, &drive9SessionReq{
		Sid:    m.sid,
		Expire: m.expireTime(),
		Info:   sinfo,
		Update: update,
	}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

func (m *drive9Meta) doRefreshSession() error {
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpRefreshSession, &drive9SessionReq{
		Sid:    m.sid,
		Expire: m.expireTime(),
	}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

func (m *drive9Meta) doFindStaleSessions(limit int) ([]uint64, error) {
	var resp drive9FindStaleResp
	st := m.call(Background(), Drive9OpFindStaleSessions, &drive9FindStaleReq{Limit: limit}, &resp)
	if st != 0 {
		return nil, st
	}
	if resp.Errno != 0 {
		return nil, syscall.Errno(resp.Errno)
	}
	return resp.Sids, nil
}

func (m *drive9Meta) doCleanStaleSession(sid uint64) error {
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpCleanStaleSession, &drive9SessionReq{Sid: sid}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

func (m *drive9Meta) doLookup(ctx Context, parent Ino, name string, inode *Ino, attr *Attr) syscall.Errno {
	var resp drive9NodeResp
	st := m.call(ctx, Drive9OpLookup, &drive9LookupReq{Parent: parent, Name: name}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	if inode != nil {
		*inode = resp.Inode
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doGetAttr(ctx Context, inode Ino, attr *Attr) syscall.Errno {
	var resp drive9NodeResp
	st := m.call(ctx, Drive9OpGetAttr, &drive9GetAttrReq{Inode: inode}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doSetAttr(ctx Context, inode Ino, set uint16, sugidclearmode uint8, attr *Attr, oldAttr *Attr) syscall.Errno {
	reqAttr := Attr{}
	if attr != nil {
		reqAttr = *attr
	}
	var resp drive9NodeResp
	st := m.call(ctx, Drive9OpSetAttr, &drive9SetAttrReq{
		Inode:          inode,
		Set:            set,
		SugidClearMode: sugidclearmode,
		Attr:           reqAttr,
	}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	copyAttr(oldAttr, resp.Attr)
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doMknod(ctx Context, parent Ino, name string, _type uint8, mode, cumask uint16, path string, inode *Ino, attr *Attr) syscall.Errno {
	reqAttr := Attr{}
	if attr != nil {
		reqAttr = *attr
	}
	var ino Ino
	if inode != nil {
		ino = *inode
	}
	var resp drive9NodeResp
	st := m.call(ctx, Drive9OpMknod, &drive9MknodReq{
		Parent:   parent,
		Name:     name,
		Type:     _type,
		Mode:     mode,
		Cumask:   cumask,
		Path:     path,
		Inode:    ino,
		Attr:     reqAttr,
		ProjPath: drive9Path(ctx),
		Uid:      ctx.Uid(),
		Gid:      ctx.Gid(),
	}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		if st == syscall.EEXIST && inode != nil {
			*inode = resp.Inode
			copyAttr(attr, resp.Attr)
		}
		return st
	}
	if inode != nil {
		*inode = resp.Inode
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doUnlink(ctx Context, parent Ino, name string, attr *Attr, skipCheckTrash ...bool) syscall.Errno {
	skip := len(skipCheckTrash) == 1 && skipCheckTrash[0]
	// JuiceFS sql_unlink checks of.IsOpen inside the same meta txn after
	// reading the edge. An extra HTTP Lookup here turned every journal
	// unlink (delete-mode COMMIT) into two RTTs. Trust the FUSE
	// Drive9OpenedKey when present; only Lookup when the caller omitted it.
	opened, openedSet := drive9Opened(ctx)
	if !openedSet && m.of != nil {
		var found Ino
		var lattr Attr
		if m.doLookup(ctx, parent, name, &found, &lattr) == 0 {
			opened = m.of.IsOpen(found)
		}
	}
	var resp drive9UnlinkResp
	st := m.call(ctx, Drive9OpUnlink, &drive9UnlinkReq{
		Parent:    parent,
		Name:      name,
		SkipTrash: skip,
		ProjPath:  drive9Path(ctx),
		Sid:       m.sid,
		Opened:    opened,
	}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doRmdir(ctx Context, parent Ino, name string, inode *Ino, attr *Attr, skipCheckTrash ...bool) syscall.Errno {
	skip := len(skipCheckTrash) == 1 && skipCheckTrash[0]
	var resp drive9NodeResp
	st := m.call(ctx, Drive9OpRmdir, &drive9RmdirReq{
		Parent:    parent,
		Name:      name,
		SkipTrash: skip,
		ProjPath:  drive9Path(ctx),
	}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	if inode != nil {
		*inode = resp.Inode
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doRename(ctx Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, flags uint32, inode, tinode *Ino, attr, tattr *Attr) syscall.Errno {
	var resp drive9RenameResp
	st := m.call(ctx, Drive9OpRename, &drive9RenameReq{
		SrcParent: parentSrc,
		SrcName:   nameSrc,
		DstParent: parentDst,
		DstName:   nameDst,
		Flags:     flags,
		SrcPath:   drive9Path(ctx),
	}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	if inode != nil {
		*inode = resp.Inode
	}
	if tinode != nil {
		*tinode = resp.Tinode
	}
	copyAttr(attr, resp.Attr)
	copyAttr(tattr, resp.Tattr)
	return 0
}

func (m *drive9Meta) doReaddir(ctx Context, inode Ino, plus uint8, entries *[]*Entry, limit int) syscall.Errno {
	var resp drive9ReaddirResp
	st := m.call(ctx, Drive9OpReaddir, &drive9ReaddirReq{Inode: inode, Plus: plus, Limit: limit}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	if entries != nil {
		*entries = resp.Entries
	}
	return 0
}

func (m *drive9Meta) doRead(ctx Context, inode Ino, indx uint32) ([]*slice, syscall.Errno) {
	var resp drive9ReadResp
	st := m.call(ctx, Drive9OpRead, &drive9ReadReq{Inode: inode, Indx: indx}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return nil, st
	}
	ss := readSliceBuf(resp.Slices)
	if ss == nil {
		ss = []*slice{}
	}
	return ss, 0
}

func (m *drive9Meta) doList(ctx Context, inode Ino) ([]*slice, syscall.Errno) {
	return m.doRead(ctx, inode, 0)
}

func (m *drive9Meta) doWrite(ctx Context, inode Ino, indx uint32, off uint32, slice Slice, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno {
	return m.writeParts(ctx, inode, indx, []WritePart{{Off: off, Slice: slice}}, mtime, numSlices, delta, attr, nil)
}

// WriteParts commits many slices of one chunk in a single HTTP meta RPC.
// Used by VFS commitThread during Flush/Fsync so a WAL checkpoint is not
// one TiDB txn per 4KiB page.
func (m *drive9Meta) WriteParts(ctx Context, inode Ino, indx uint32, parts []WritePart, mtime time.Time) syscall.Errno {
	return m.enqueueWrite(inode, indx, parts, mtime, true)
}

// QueueWriteParts is JuiceFS commitThread without waiting for HTTP: SQL
// Meta.Write returns in µs so streaming commits do not stall Write().
// Fsync/Flush uses WriteParts (wait=true) so durability still matches
// JuiceFS --writeback (fsync waits for meta, not object PUT).
func (m *drive9Meta) QueueWriteParts(ctx Context, inode Ino, indx uint32, parts []WritePart, mtime time.Time) syscall.Errno {
	return m.enqueueWrite(inode, indx, parts, mtime, false)
}

func (m *drive9Meta) inodeWriter(inode Ino) *drive9InodeWriter {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if m.writers == nil {
		m.writers = make(map[Ino]*drive9InodeWriter)
	}
	w := m.writers[inode]
	if w == nil {
		w = &drive9InodeWriter{q: make(chan drive9WriteJob, 256)}
		m.writers[inode] = w
		go m.inodeWriteWorker(w)
	}
	return w
}

func (m *drive9Meta) enqueueWrite(inode Ino, indx uint32, parts []WritePart, mtime time.Time, wait bool) syscall.Errno {
	if len(parts) == 0 && !wait {
		return 0
	}
	copied := append([]WritePart(nil), parts...)
	job := drive9WriteJob{inode: inode, indx: indx, parts: copied, mtime: mtime}
	if wait {
		job.done = make(chan syscall.Errno, 1)
	}
	w := m.inodeWriter(inode)
	w.pending.Add(1)
	w.seq.Add(1)
	w.q <- job
	if !wait {
		return 0
	}
	st := <-job.done
	return st
}

// WriteState reports an inode's metadata commit state: how many enqueued
// commits have not landed yet, and a sequence number that changes on every
// enqueue. A reader that still holds the written bytes in its buffer may
// answer from it; one whose bytes already went to the object store may not,
// because the slice only becomes readable when its commit lands. Meta engines
// that cannot answer keep the caller's Flush.
func (m *drive9Meta) WriteState(inode Ino) (pending int64, seq uint64) {
	m.writeMu.Lock()
	w := m.writers[inode]
	m.writeMu.Unlock()
	if w == nil {
		return 0, 0
	}
	return w.pending.Load(), w.seq.Load()
}

// WaitWrites is JuiceFS Flush: return only after this inode's Meta.Write
// HTTP has finished. A no-op job sits behind queued writes of this inode, and
// the inode's sticky commit error is reported after it: a queued batch that
// failed earlier has nobody to return to, so without this a Flush would answer
// success for data metadata never referenced.
func (m *drive9Meta) WaitWrites(inode Ino) syscall.Errno {
	st := m.enqueueWrite(inode, 0, nil, time.Time{}, true)
	if st != 0 {
		return st
	}
	m.writeMu.Lock()
	w := m.writers[inode]
	m.writeMu.Unlock()
	return w.takeCommitError()
}

func (m *drive9Meta) inodeWriteWorker(w *drive9InodeWriter) {
	for job := range w.q {
		if len(job.parts) == 0 {
			m.signalWriteBatch([]drive9WriteJob{job}, 0)
			continue
		}
		batch := []drive9WriteJob{job}
		parts := append([]WritePart(nil), job.parts...)
		mtime := job.mtime
		indx := job.indx
		// JuiceFS SQL Meta.Write is µs so commitThread needs no delay.
		// HTTP ~20–80ms: collect a few milliseconds so sqlite 4KiB spills
		// under DELETE exclusive become one WriteParts, not one HTTP each.
		timer := time.NewTimer(8 * time.Millisecond)
		collect := job.done == nil && len(parts) < 64
		for collect {
			select {
			case j, ok := <-w.q:
				if !ok {
					collect = false
					break
				}
				if len(j.parts) == 0 {
					st := m.commitWriteParts(job.inode, indx, parts, mtime)
					m.signalWriteBatch(batch, st)
					m.signalWriteBatch([]drive9WriteJob{j}, st)
					batch = nil
					parts = nil
					collect = false
				} else if j.indx == indx {
					batch = append(batch, j)
					parts = append(parts, j.parts...)
					if j.mtime.After(mtime) {
						mtime = j.mtime
					}
					if j.done != nil || len(parts) >= 64 {
						collect = false
					}
				} else {
					st := m.commitWriteParts(job.inode, indx, parts, mtime)
					m.signalWriteBatch(batch, st)
					batch = []drive9WriteJob{j}
					parts = append([]WritePart(nil), j.parts...)
					mtime = j.mtime
					indx = j.indx
					job = j
					collect = j.done == nil && len(parts) < 64
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(8 * time.Millisecond)
				}
			case <-timer.C:
				collect = false
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if len(parts) > 0 {
			st := m.commitWriteParts(job.inode, indx, parts, mtime)
			m.signalWriteBatch(batch, st)
		}
	}
}

func (m *drive9Meta) signalWriteBatch(batch []drive9WriteJob, st syscall.Errno) {
	if len(batch) == 0 {
		return
	}
	// A batch is always one inode's jobs: the worker drains a single queue.
	w := m.inodeWriter(batch[0].inode)
	w.recordCommitError(st)
	for _, j := range batch {
		w.pending.Add(-1)
		if j.done != nil {
			j.done <- st
		}
	}
}

func (m *drive9Meta) commitWriteParts(inode Ino, indx uint32, parts []WritePart, mtime time.Time) syscall.Errno {
	if len(parts) == 0 {
		return 0
	}
	f := m.of.find(inode)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	var numSlices int
	var delta dirStat
	var attr Attr
	var written []byte
	st := m.writeParts(Background(), inode, indx, parts, mtime, &numSlices, &delta, &attr, &written)
	if st == 0 {
		// Keep the chunk mapping this write produced instead of dropping it:
		// the server serialised it inside the same transaction, so it is
		// exactly what a follow-up read would fetch, and a chunk spans 64 MB
		// (a whole sqlite database), so dropping it costs one HTTP metadata
		// query per read for the rest of the workload. Anything else that can
		// change the mapping (truncate, compact) invalidates it itself.
		if ss := readSliceBuf(written); len(ss) > 0 {
			m.of.CacheChunk(inode, indx, buildSlice(ss))
			m.noteChunkCached(inode, indx)
			// InvalidateChunk also drops the open file's cached attributes, and
			// the write that just happened is exactly when they are stale: a
			// freshly created file caches Length=0, and keeping the mapping must
			// not keep that length. Invalidate the attributes only.
			m.of.InvalidateChunk(inode, invalidateAttrOnly)
		} else {
			m.of.InvalidateChunk(inode, indx)
		}
		m.updateParentStat(Background(), inode, attr.Parent, delta.length, delta.space)
		m.updateUserGroupStat(Background(), attr.Uid, attr.Gid, delta.space, 0)
		// JuiceFS baseMeta.Write also compactChunk at numSlices>350, but
		// that is a local SQL txn (µs). HTTP compact CAS is 50–400ms and
		// serialized with sqlite fsync on the same chunk (crash01 --wait
		// all). Bound the blob like JuiceFS maxSlices; do it asynchronously
		// so Write/Flush is not the compact HTTP.
		if numSlices >= maxSlices {
			go m.compactChunk(inode, indx, true, false, int(attr.Tier))
		}
	}
	return st
}

// Write is WriteParts of one slice so ncommit=1 does not use baseMeta.Write's
// inline compactChunk (NoBGJob: compact is the drive9 compact loop).
func (m *drive9Meta) Write(ctx Context, inode Ino, indx uint32, off uint32, slice Slice, mtime time.Time) syscall.Errno {
	return m.WriteParts(ctx, inode, indx, []WritePart{{Off: off, Slice: slice}}, mtime)
}

// Truncate drains this inode's queued slice commits before the truncate RPC.
// The server derives the zeroed range from the stored length, so a commit
// still sitting in the async write queue would be zeroed by a truncate-up
// (or leave a stale length) even though the kernel already completed the
// write. JuiceFS's synchronous Meta.Write made that ordering implicit.
func (m *drive9Meta) Truncate(ctx Context, inode Ino, flags uint8, length uint64, attr *Attr, skipPermCheck bool) syscall.Errno {
	if st := m.WaitWrites(inode); st != 0 {
		return st
	}
	return m.baseMeta.Truncate(ctx, inode, flags, length, attr, skipPermCheck)
}

// Read is baseMeta.Read without compactChunk. JuiceFS launches compact when a
// chunk has ≥5 slices because SQL compact is µs; HTTP compact CAS contended
// with sqlite exclusive (575 compact RPCs on community.sqlite).
func (m *drive9Meta) Read(ctx Context, inode Ino, indx uint32, slices *[]Slice) syscall.Errno {
	f := m.of.find(inode)
	if f != nil {
		f.RLock()
		defer f.RUnlock()
	}
	if ss, ok := m.of.ReadChunk(inode, indx); ok && m.chunkCacheFresh(inode, indx) {
		*slices = ss
		return 0
	}
	*slices = nil
	ss, st := m.en.doRead(ctx, inode, indx)
	if st != 0 {
		return st
	}
	if ss == nil {
		return syscall.EIO
	}
	if len(ss) == 0 {
		var attr Attr
		if st = m.en.doGetAttr(ctx, inode, &attr); st != 0 {
			return st
		}
		if attr.Typ != TypeFile {
			return syscall.EPERM
		}
		return 0
	}
	*slices = buildSlice(ss)
	m.of.CacheChunk(inode, indx, *slices)
	m.noteChunkCached(inode, indx)
	return 0
}

func (m *drive9Meta) writeParts(ctx Context, inode Ino, indx uint32, parts []WritePart, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr, written *[]byte) syscall.Errno {
	if len(parts) == 0 {
		return 0
	}
	req := &drive9WriteReq{Inode: inode, Indx: indx, Mtime: mtime, Parts: parts}
	if len(parts) == 1 {
		req.Off = parts[0].Off
		req.Slice = parts[0].Slice
		req.Parts = nil
	}
	var resp drive9WriteResp
	st := m.call(ctx, Drive9OpWrite, req, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	if written != nil {
		*written = resp.Slices
	}
	if numSlices != nil {
		*numSlices = resp.NumSlices
	}
	if delta != nil {
		delta.length = resp.Length
		delta.space = resp.Space
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doTruncate(ctx Context, inode Ino, flags uint8, length uint64, delta *dirStat, attr *Attr, skipPermCheck bool) syscall.Errno {
	var resp drive9TruncateResp
	st := m.call(ctx, Drive9OpTruncate, &drive9TruncateReq{
		Inode:         inode,
		Flags:         flags,
		Length:        length,
		SkipPermCheck: skipPermCheck,
	}, &resp)
	if st = respErrno(resp.Errno, st); st != 0 {
		return st
	}
	if delta != nil {
		delta.length = resp.Length
		delta.space = resp.Space
	}
	copyAttr(attr, resp.Attr)
	return 0
}

func (m *drive9Meta) doCompactChunk(inode Ino, indx uint32, origin []byte, ss []*slice, skipped int, pos uint32, id uint64, size uint32, delayed []byte) syscall.Errno {
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpCompact, &drive9CompactReq{
		Inode:   inode,
		Indx:    indx,
		Origin:  origin,
		Skipped: skipped,
		Pos:     pos,
		Id:      id,
		Size:    size,
		Delayed: delayed,
	}, &resp)
	return respErrno(resp.Errno, st)
}

func (m *drive9Meta) doDeleteSlice(id uint64, size uint32) error {
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpDeleteSlice, &drive9DeleteSliceReq{Id: id, Size: size}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

func (m *drive9Meta) doDeleteSustainedInode(sid uint64, inode Ino) error {
	var resp drive9ErrnoResp
	st := m.call(Background(), Drive9OpDeleteSustained, &drive9SustainedReq{Sid: sid, Inode: inode}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

func (m *drive9Meta) GetSession(sid uint64, detail bool) (*Session, error) {
	var resp drive9GetSessionResp
	st := m.call(Background(), Drive9OpGetSession, &drive9GetSessionReq{Sid: sid, Detail: detail}, &resp)
	if st != 0 {
		return nil, st
	}
	if resp.Errno != 0 {
		return nil, syscall.Errno(resp.Errno)
	}
	return resp.Session, nil
}

func (m *drive9Meta) newDirHandler(inode Ino, plus bool, entries []*Entry) DirHandler {
	var list []*Entry
	_ = m.doReaddir(Background(), inode, 1, &list, -1)
	all := append([]*Entry(nil), entries...)
	all = append(all, list...)
	return &staticDirHandler{entries: all}
}

func (m *drive9Meta) scanAllChunks(_ Context, _ chan<- cchunk, _ *utils.Bar) error {
	return nil
}

func (m *drive9Meta) doFindDeletedFiles(_ int64, _ int) (map[Ino]uint64, error) {
	return map[Ino]uint64{}, nil
}

func (m *drive9Meta) doDeleteFileData(Ino, uint64) {}

func (m *drive9Meta) doCleanupSlices(_ Context, _ *uint64) error { return nil }

func (m *drive9Meta) doCleanupDelayedSlices(_ Context, _ int64) (int, error) { return 0, nil }

func (m *drive9Meta) doCloneEntry(_ Context, _ Ino, _ Ino, _ string, _ Ino, _ *Attr, _ uint8, _ uint16, _ bool) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doBatchClone(_ Context, _ Ino, _ Ino, _ []*Entry, _ uint8, _ uint16, _ *batchCloneResult) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doAttachDirNode(_ Context, _ Ino, _ Ino, _ string) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doFindDetachedNodes(_ time.Time) []Ino { return nil }

func (m *drive9Meta) doCleanupDetachedNode(_ Context, _ Ino) syscall.Errno { return 0 }

func (m *drive9Meta) doScanSustainedInodes(_ Context, _ func(uid, gid uint32, length uint64) error) error {
	return nil
}

func (m *drive9Meta) doGetQuota(_ Context, _ uint32, _ uint64) (*Quota, error) { return nil, nil }

func (m *drive9Meta) doSetQuota(_ Context, _ uint32, _ uint64, _ *Quota) (bool, error) {
	return false, nil
}

func (m *drive9Meta) doDelQuota(_ Context, _ uint32, _ uint64) error { return nil }

func (m *drive9Meta) doLoadQuotas(_ Context) (map[uint64]*Quota, map[uint64]*Quota, map[uint64]*Quota, error) {
	return nil, nil, nil, nil
}

func (m *drive9Meta) doFlushQuotas(_ Context, _ []*iQuota) error { return nil }

func (m *drive9Meta) cleanUgUsage(_ Context, _ uint32) error { return nil }

func (m *drive9Meta) doLink(_ Context, _, _ Ino, _ string, _ *Attr) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doBatchUnlink(ctx Context, parent Ino, entries []*Entry, delta *dirStat, skipCheckTrash ...bool) syscall.Errno {
	if delta != nil {
		*delta = dirStat{}
	}
	for _, e := range entries {
		var attr Attr
		if st := m.doUnlink(ctx, parent, string(e.Name), &attr, skipCheckTrash...); st != 0 {
			return st
		}
	}
	return 0
}

func (m *drive9Meta) doReadlink(_ Context, _ Ino, _ bool) (int64, []byte, error) {
	return 0, nil, syscall.ENOSYS
}

func (m *drive9Meta) doSetXattr(_ Context, _ Ino, _ string, _ []byte, _ uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doRemoveXattr(_ Context, _ Ino, _ string) syscall.Errno { return syscall.ENOSYS }

func (m *drive9Meta) doRepair(_ Context, _ Ino, _ *Attr) syscall.Errno { return 0 }

func (m *drive9Meta) doTouchAtime(_ Context, _ Ino, _ *Attr, _ time.Time) (bool, error) {
	return false, nil
}

func (m *drive9Meta) doFallocate(_ Context, _ Ino, _ uint8, _ uint64, _ uint64, _ *dirStat, _ *Attr) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doGetParents(_ Context, _ Ino) map[Ino]int { return map[Ino]int{} }

func (m *drive9Meta) doUpdateDirStat(_ Context, _ map[Ino]dirStat) error { return nil }

func (m *drive9Meta) doGetDirStat(_ Context, _ Ino, _ bool) (*dirStat, syscall.Errno) {
	return &dirStat{}, 0
}

func (m *drive9Meta) doSyncDirStat(_ Context, _ Ino) (*dirStat, syscall.Errno) {
	return &dirStat{}, 0
}

func (m *drive9Meta) doSyncVolumeStat(_ Context, _, _ int64) error { return nil }

func (m *drive9Meta) scanTrashSlices(Context, trashSliceScan) error     { return nil }
func (m *drive9Meta) scanPendingSlices(Context, pendingSliceScan) error { return nil }
func (m *drive9Meta) scanPendingFiles(Context, pendingFileScan) error   { return nil }

func (m *drive9Meta) doSetFacl(_ Context, _ Ino, _ uint8, _ *aclAPI.Rule) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doGetFacl(_ Context, _ Ino, _ uint8, _ uint32, _ *aclAPI.Rule) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) cacheACLs(_ Context) error { return nil }

func (m *drive9Meta) doStoreToken(_ Context, _ []byte) (uint32, syscall.Errno) {
	return 0, syscall.ENOSYS
}

func (m *drive9Meta) doUpdateToken(_ Context, _ uint32, _ []byte) syscall.Errno {
	return syscall.ENOSYS
}

func (m *drive9Meta) doLoadToken(_ Context, _ uint32) ([]byte, syscall.Errno) {
	return nil, syscall.ENOSYS
}

func (m *drive9Meta) doDeleteTokens(_ Context, _ []uint32) syscall.Errno { return 0 }

func (m *drive9Meta) doListTokens(_ Context) (map[uint32][]byte, syscall.Errno) {
	return map[uint32][]byte{}, 0
}

func (m *drive9Meta) doCleanupChangelog(_ Context, _ time.Duration, _ int64) error { return nil }

func (m *drive9Meta) dump(_ Context, _ *DumpOption, _ chan<- *dumpedResult) error {
	return syscall.ENOSYS
}

func (m *drive9Meta) load(_ Context, _ int, _ *LoadOption, _ proto.Message) error {
	return syscall.ENOSYS
}

func (m *drive9Meta) prepareLoad(_ Context, _ *LoadOption) error { return syscall.ENOSYS }
