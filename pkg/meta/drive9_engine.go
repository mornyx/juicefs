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
	opened := drive9Opened(ctx)
	if !opened && m.of != nil {
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
	return m.writeParts(ctx, inode, indx, []WritePart{{Off: off, Slice: slice}}, mtime, numSlices, delta, attr)
}

// WriteParts commits many slices of one chunk in a single HTTP meta RPC.
// Used by VFS commitThread during Flush/Fsync so a WAL checkpoint is not
// one TiDB txn per 4KiB page.
func (m *drive9Meta) WriteParts(ctx Context, inode Ino, indx uint32, parts []WritePart, mtime time.Time) syscall.Errno {
	if len(parts) == 0 {
		return 0
	}
	f := m.of.find(inode)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	defer func() { m.of.InvalidateChunk(inode, indx) }()
	var numSlices int
	var delta dirStat
	var attr Attr
	st := m.writeParts(ctx, inode, indx, parts, mtime, &numSlices, &delta, &attr)
	if st == 0 {
		m.updateParentStat(ctx, inode, attr.Parent, delta.length, delta.space)
		m.updateUserGroupStat(ctx, attr.Uid, attr.Gid, delta.space, 0)
		if numSlices%100 == 99 || numSlices > 350 {
			if numSlices < maxSlices {
				go m.compactChunk(inode, indx, false, false, int(attr.Tier))
			} else {
				m.compactChunk(inode, indx, true, false, int(attr.Tier))
			}
		}
	}
	return st
}

func (m *drive9Meta) writeParts(ctx Context, inode Ino, indx uint32, parts []WritePart, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno {
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
