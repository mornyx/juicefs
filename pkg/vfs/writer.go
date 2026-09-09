/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package vfs

import (
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	flushDuration = time.Second * 5
	// errSliceSealed is internal: writeback_cache rewrote a FlushTo'd page.
	errSliceSealed = syscall.Errno(0x5a5a)
)

type FileWriter interface {
	Write(ctx meta.Context, offset uint64, data []byte) syscall.Errno
	Flush(ctx meta.Context) syscall.Errno
	Close(ctx meta.Context) syscall.Errno
	GetLength() uint64
	Truncate(length uint64)
}

type DataWriter interface {
	Open(inode Ino, fleng uint64, tierID uint8) FileWriter
	Flush(ctx meta.Context, inode Ino) syscall.Errno
	GetLength(inode Ino) uint64
	Truncate(inode Ino, length uint64)
	UpdateMtime(inode Ino, mtime time.Time)
	FlushAll() error
}

type sliceWriter struct {
	id      uint64
	chunk   *chunkWriter
	off     uint32
	length  uint32
	soff    uint32
	slen    uint32
	writer  chunk.Writer
	freezed bool
	done    bool
	err     syscall.Errno
	notify  *utils.Cond
	started time.Time
	lastMod time.Time

	growing   bool
	committed bool
	dep       *sliceWriter
}

func (s *sliceWriter) prepareID(ctx meta.Context, retry bool) {
	f := s.chunk.file
	f.Lock()
	for s.id == 0 {
		var id uint64
		f.Unlock()
		st := f.w.m.NewSlice(ctx, &id)
		f.Lock()
		if st != 0 && st != syscall.EIO {
			s.err = st
			break
		}
		if !retry || st == 0 {
			if s.id == 0 {
				s.id = id
			}
			break
		}
		f.Unlock()
		logger.Debugf("meta is not available: %s", st)
		time.Sleep(time.Millisecond * 100)
		f.Lock()
	}
	if s.writer != nil && s.writer.ID() == 0 {
		s.writer.SetID(s.id)
	}
	f.Unlock()
}

func (s *sliceWriter) markDone() {
	f := s.chunk.file
	f.Lock()
	s.done = true
	s.notify.Signal()
	f.Unlock()
}

// freezed, no more data
func (s *sliceWriter) flushData() {
	defer s.markDone()
	if s.slen == 0 {
		return
	}
	s.prepareID(meta.Background(), true)
	if s.err != 0 {
		logger.Infof("flush inode: %v chunk: %d err: %s", s.chunk.file.inode, s.id, s.err)
		s.writer.Abort()
		return
	}
	s.length = s.slen
	if err := s.writer.Finish(int(s.length)); err != nil {
		logger.Errorf("upload inode: %v chunk: %v (length: %v) fail: %s", s.chunk.file.inode, s.id, s.length, err)

		s.writer.Abort()
		s.err = syscall.EIO
	}
}

// protected by s.chunk.file
func (s *sliceWriter) write(ctx meta.Context, off uint32, data []uint8) syscall.Errno {
	f := s.chunk.file
	_, err := s.writer.WriteAt(data, int64(off))
	if err != nil {
		if errors.Is(err, chunk.ErrOverwriteUploaded) {
			// JuiceFS `-o writeback_cache`: kernel rewrites a staged page.
			return errSliceSealed
		}
		logger.Warnf("write inode: %v chunk: %d off: %d %s", s.chunk.file.inode, s.id, off, err)
		return syscall.EIO
	}
	if off+uint32(len(data)) > s.slen {
		s.slen = off + uint32(len(data))
	}
	s.lastMod = time.Now()
	if s.slen == meta.ChunkSize {
		s.freezed = true
		go s.flushData()
	} else if int(s.slen) >= f.w.blockSize {
		if s.id > 0 {
			err := s.writer.FlushTo(int(s.slen))
			if err != nil {
				logger.Warnf("write inode: %v chunk: %d off: %d %s", s.chunk.file.inode, s.id, off, err)
				return syscall.EIO
			}
		}
	}
	return 0
}

type chunkWriter struct {
	indx   uint32
	file   *fileWriter
	slices []*sliceWriter
}

// protected by file
func (c *chunkWriter) findWritableSlice(pos uint32, size uint32) *sliceWriter {
	blockSize := uint32(c.file.w.blockSize)
	for i := range c.slices {
		s := c.slices[len(c.slices)-1-i]
		if !s.freezed {
			flushoff := s.slen / blockSize * blockSize
			if pos >= s.off+flushoff && pos <= s.off+s.slen {
				return s
			} else if i > 3 {
				s.freezed = true
				go s.flushData()
			}
		}
		if pos < s.off+s.slen && s.off < pos+size {
			// overlaped
			// TODO: write into multiple slices
			return nil
		}
	}
	return nil
}

type metaWriteParts interface {
	WriteParts(ctx meta.Context, inode Ino, indx uint32, parts []meta.WritePart, mtime time.Time) syscall.Errno
}

type metaQueueWriteParts interface {
	QueueWriteParts(ctx meta.Context, inode Ino, indx uint32, parts []meta.WritePart, mtime time.Time) syscall.Errno
}

// metaWaitWrites is drive9 HTTP meta: JuiceFS Flush waits until Meta.Write
// has committed. Streaming commitThread uses QueueWriteParts so Write() is
// not stalled (JuiceFS SQL Meta.Write is µs); Flush drains the inode queue.
type metaWaitWrites interface {
	WaitWrites(inode Ino) syscall.Errno
}

func writeMetaParts(m meta.Meta, inode Ino, indx uint32, items []*sliceWriter) syscall.Errno {
	for _, it := range items {
		if it == nil {
			continue
		}
		if err := m.Write(meta.Background(), inode, indx, it.off, meta.Slice{Id: it.id, Size: it.length, Off: it.soff, Len: it.slen}, it.lastMod); err != 0 {
			return err
		}
	}
	return 0
}

func (c *chunkWriter) commitItems(f *fileWriter, items []*sliceWriter) syscall.Errno {
	if len(items) == 0 {
		return 0
	}
	parts := make([]meta.WritePart, len(items))
	var lastMod time.Time
	for i, it := range items {
		parts[i] = meta.WritePart{Off: it.off, Slice: meta.Slice{Id: it.id, Size: it.length, Off: it.soff, Len: it.slen}}
		if it.lastMod.After(lastMod) {
			lastMod = it.lastMod
		}
	}
	wait := f.flushwaiting > 0
	var err syscall.Errno
	if !wait {
		if qw, ok := f.w.m.(metaQueueWriteParts); ok {
			err = qw.QueueWriteParts(meta.Background(), f.inode, c.indx, parts, lastMod)
		} else if bw, ok := f.w.m.(metaWriteParts); ok && len(parts) > 1 {
			err = bw.WriteParts(meta.Background(), f.inode, c.indx, parts, lastMod)
		} else {
			err = writeMetaParts(f.w.m, f.inode, c.indx, items)
		}
	} else if bw, ok := f.w.m.(metaWriteParts); ok {
		err = bw.WriteParts(meta.Background(), f.inode, c.indx, parts, lastMod)
	} else {
		err = writeMetaParts(f.w.m, f.inode, c.indx, items)
	}
	if err == 0 {
		for _, it := range items {
			f.w.reader.Invalidate(f.inode, uint64(c.indx)*meta.ChunkSize+uint64(it.off), uint64(it.slen))
		}
	}
	return err
}

func (c *chunkWriter) commitThread() {
	f := c.file
	defer f.w.free(f)
	f.Lock()

	// the slices should be committed in the order that are created
	for len(c.slices) > 0 {
		s := c.slices[0]
		for !s.done {
			if s.notify.WaitWithTimeout(time.Millisecond*100) && !s.freezed && time.Since(s.started) > flushDuration*2 {
				s.freezed = true
				go s.flushData()
			}
		}
		for s.dep != nil && !s.dep.committed {
			f.commitcond.WaitWithTimeout(time.Millisecond * 100)
		}
		ncommit := c.pickCommitCount()
		err := s.err
		for i := 0; i < ncommit; i++ {
			if c.slices[i].err != 0 {
				err = c.slices[i].err
				ncommit = i
				break
			}
			if c.slices[i].dep != nil && !c.slices[i].dep.committed {
				ncommit = i
				break
			}
		}
		if ncommit < 1 {
			ncommit = 1
		}
		items := append([]*sliceWriter(nil), c.slices[:ncommit]...)
		f.Unlock()

		if err == 0 {
			err = c.commitItems(f, items)
		}

		f.Lock()
		if err != 0 {
			if err == syscall.ENOENT || err == syscall.ENOSPC || err == syscall.EDQUOT {
				for _, it := range items {
					id, length := it.id, int(it.length)
					go func(id uint64, length int) {
						_ = f.w.store.Remove(id, length)
					}(id, length)
				}
			} else {
				logger.Warnf("write inode:%d error: %s", f.inode, err)
				err = syscall.EIO
			}
			f.err = err
			logger.Errorf("write inode:%d indx:%d %s", f.inode, c.indx, err)
		}
		for i := 0; i < ncommit && i < len(c.slices); i++ {
			c.slices[i].committed = true
			if c.slices[i].growing {
				f.commitcond.Broadcast()
			}
		}
		c.slices = c.slices[ncommit:]
	}
	f.freeChunk(c)
	f.Unlock()
}

// writePartsBatch is the HTTP-meta coalescing ceiling. JuiceFS SQL Meta.Write
// is cheap so commitThread used ncommit=1 except during Flush. drive9's
// WriteParts is one RTT; sqlite page writes must not be one HTTP each.
const writePartsBatch = 64

func leadingDoneSlices(slices []*sliceWriter) int {
	n := 0
	for _, x := range slices {
		if x == nil || !x.done || x.err != 0 {
			break
		}
		n++
	}
	return n
}

func frozenInflight(slices []*sliceWriter) int {
	n := 0
	for _, s := range slices {
		if s != nil && s.freezed && !s.done {
			n++
		}
	}
	return n
}

func clampCommitCount(n int) int {
	if n < 1 {
		return 1
	}
	if n > writePartsBatch {
		return writePartsBatch
	}
	return n
}

// pickCommitCount is how many leading slices commitThread sends in one
// Meta.Write / WriteParts. Caller holds f.Lock.
//
// JuiceFS SQL commits each frozen slice immediately (µs). HTTP meta must
// still commit as slices freeze (delay-until-Flush made WAL --sync Flush
// too heavy and failed wal-multiwrite01). Coalesce only slices whose
// flushData is already in flight so sqlite 4KiB spills become one RTT.
func (c *chunkWriter) pickCommitCount() int {
	f := c.file
	if f.flushwaiting > 0 {
		for {
			ready := true
			for _, x := range c.slices {
				if !x.done {
					ready = false
					break
				}
			}
			if ready {
				return len(c.slices)
			}
			f.commitcond.WaitWithTimeout(time.Millisecond * 10)
		}
	}
	if _, ok := f.w.m.(metaWriteParts); !ok {
		return 1
	}
	deadline := time.Now().Add(5 * time.Millisecond)
	for {
		n := leadingDoneSlices(c.slices)
		if frozenInflight(c.slices) == 0 || n >= writePartsBatch || n == len(c.slices) || time.Now().After(deadline) {
			return clampCommitCount(n)
		}
		f.commitcond.WaitWithTimeout(time.Millisecond)
	}
}

type fileWriter struct {
	sync.Mutex
	w *dataWriter

	inode        Ino
	length       uint64
	tierID       uint8
	err          syscall.Errno
	flushwaiting uint16
	writewaiting uint16
	refs         uint16
	chunks       map[uint32]*chunkWriter

	flushcond  *utils.Cond // wait for chunks==nil (flush)
	writecond  *utils.Cond // wait for flushwaiting==0 (write)
	commitcond *utils.Cond // wait for committed==true of dependency slice (commit)
}

// protected by file
func (f *fileWriter) findChunk(i uint32) *chunkWriter {
	c := f.chunks[i]
	if c == nil {
		c = &chunkWriter{indx: i, file: f}
		f.chunks[i] = c
	}
	return c
}

// protected by file
func (f *fileWriter) freeChunk(c *chunkWriter) {
	delete(f.chunks, c.indx)
	if len(f.chunks) == 0 && f.flushwaiting > 0 {
		f.flushcond.Broadcast()
	}
}

func (f *fileWriter) newSliceWriter(c *chunkWriter, off uint32) *sliceWriter {
	s := &sliceWriter{
		chunk:   c,
		off:     off,
		writer:  f.w.store.NewWriter(0, f.tierID),
		notify:  utils.NewCond(&f.Mutex),
		started: time.Now(),
	}
	go s.prepareID(meta.Background(), false)
	c.slices = append(c.slices, s)
	if len(c.slices) == 1 {
		f.w.Lock()
		f.refs++
		f.w.Unlock()
		go c.commitThread()
		if uint64(c.indx)*meta.ChunkSize >= f.length {
			// first slice of a new chunk, try to find the last slice of the last chunk as dependency
			var lastChunk *chunkWriter
			for i, oc := range f.chunks {
				if i < c.indx && (lastChunk == nil || i > lastChunk.indx) {
					lastChunk = oc
				}
			}
			if lastChunk != nil {
				var lastSlice *sliceWriter
				for _, ls := range lastChunk.slices {
					if ls.growing {
						lastSlice = ls
					}
				}
				s.dep = lastSlice
			}
		}
	}
	return s
}

// protected by file
func (f *fileWriter) writeChunk(ctx meta.Context, indx uint32, off uint32, data []byte) syscall.Errno {
	c := f.findChunk(indx)
	s := c.findWritableSlice(off, uint32(len(data)))
	if s == nil {
		s = f.newSliceWriter(c, off)
	}
	if !s.growing && uint64(indx)*meta.ChunkSize+uint64(off)+uint64(len(data)) > f.length {
		s.growing = true
	}
	st := s.write(ctx, off-s.off, data)
	if st != errSliceSealed {
		return st
	}
	// FUSE writeback_cache turned a sequential slice into a random rewrite
	// of an already staged block. JuiceFS would also open a new slice
	// (findWritableSlice overlap → nil); we missed the uploaded-prefix case.
	s.freezed = true
	go s.flushData()
	s = f.newSliceWriter(c, off)
	if !s.growing && uint64(indx)*meta.ChunkSize+uint64(off)+uint64(len(data)) > f.length {
		s.growing = true
	}
	return s.write(ctx, off-s.off, data)
}

func (f *fileWriter) totalSlices() int {
	var cnt int
	f.Lock()
	for _, c := range f.chunks {
		cnt += len(c.slices)
	}
	f.Unlock()
	return cnt
}

func (w *dataWriter) usedBufferSize() int64 {
	return utils.AllocMemory() - w.store.UsedMemory()
}

func (f *fileWriter) Write(ctx meta.Context, off uint64, data []byte) syscall.Errno {
	for f.totalSlices() >= 1000 {
		time.Sleep(time.Millisecond)
	}
	if f.w.usedBufferSize() > f.w.bufferSize {
		// slow down
		time.Sleep(time.Millisecond * 10)
		for f.w.usedBufferSize() > f.w.bufferSize*2 {
			time.Sleep(time.Millisecond * 100)
		}
	}

	s := time.Now()
	f.Lock()
	defer f.Unlock()
	size := uint64(len(data))
	f.writewaiting++
	for f.flushwaiting > 0 {
		if f.writecond.WaitWithTimeout(time.Second) && ctx.Canceled() {
			f.writewaiting--
			logger.Warnf("write %d interrupted after %d", f.inode, time.Since(s))
			return syscall.EINTR
		}
	}
	f.writewaiting--

	indx := uint32(off / meta.ChunkSize)
	pos := uint32(off % meta.ChunkSize)
	for len(data) > 0 {
		n := uint32(len(data))
		if pos+n > meta.ChunkSize {
			n = meta.ChunkSize - pos
		}
		if st := f.writeChunk(ctx, indx, pos, data[:n]); st != 0 {
			return st
		}
		data = data[n:]
		indx++
		pos = (pos + n) % meta.ChunkSize
	}
	if off+size > f.length {
		f.length = off + size
	}
	return f.err
}

func (f *fileWriter) updateMtime(t time.Time) {
	f.Lock()
	defer f.Unlock()
	for _, c := range f.chunks {
		for _, s := range c.slices {
			s.lastMod = t
		}
	}
}

func (f *fileWriter) flush(ctx meta.Context, writeback bool) syscall.Errno {
	s := time.Now()
	f.Lock()
	defer f.Unlock()
	f.flushwaiting++

	var err syscall.Errno
	var wait = time.Second * time.Duration((f.w.maxRetries+2)*(f.w.maxRetries+2)/2)
	if wait < time.Minute*5 {
		wait = time.Minute * 5
	}
	var deadline = time.Now().Add(wait)
	for len(f.chunks) > 0 && err == 0 {
		for _, c := range f.chunks {
			for _, s := range c.slices {
				if !s.freezed {
					s.freezed = true
					go s.flushData()
				}
			}
		}
		if f.flushcond.WaitWithTimeout(time.Second*3) && ctx.Canceled() && time.Since(s) > f.w.conf.Chunk.PutTimeout*2 {
			logger.Warnf("flush %d interrupted after %d", f.inode, time.Since(s))
			err = syscall.EINTR
			break
		}
		if time.Now().After(deadline) {
			logger.Errorf("flush %d timeout after waited %s", f.inode, wait)
			for _, c := range f.chunks {
				for _, s := range c.slices {
					logger.Errorf("pending slice %d-%d: %+v", f.inode, c.indx, *s)
				}
			}
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			logger.Warnf("All goroutines (%d):\n%s", runtime.NumGoroutine(), buf[:n])
			err = syscall.EIO
			break
		}
	}
	if err == 0 {
		err = f.err
	}
	if err == 0 {
		if w, ok := f.w.m.(metaWaitWrites); ok {
			f.Unlock()
			err = w.WaitWrites(f.inode)
			f.Lock()
		}
	}
	f.flushwaiting--
	if f.flushwaiting == 0 && f.writewaiting > 0 {
		f.writecond.Broadcast()
	}
	return err
}

func (f *fileWriter) Flush(ctx meta.Context) syscall.Errno {
	return f.flush(ctx, false)
}

func (f *fileWriter) Close(ctx meta.Context) syscall.Errno {
	defer f.w.free(f)
	return f.Flush(ctx)
}

func (f *fileWriter) GetLength() uint64 {
	f.Lock()
	defer f.Unlock()
	return f.length
}

func (f *fileWriter) Truncate(length uint64) {
	f.Lock()
	defer f.Unlock()
	// TODO: truncate write buffer if length < f.length
	f.length = length
}

type dataWriter struct {
	sync.Mutex
	m          meta.Meta
	store      chunk.ChunkStore
	conf       *Config
	reader     DataReader
	blockSize  int
	bufferSize int64
	files      map[Ino]*fileWriter
	maxRetries uint32
}

func NewDataWriter(conf *Config, m meta.Meta, store chunk.ChunkStore, reader DataReader) DataWriter {
	w := &dataWriter{
		m:          m,
		store:      store,
		reader:     reader,
		conf:       conf,
		blockSize:  conf.Chunk.BlockSize,
		bufferSize: int64(conf.Chunk.BufferSize),
		files:      make(map[Ino]*fileWriter),
		maxRetries: uint32(conf.Meta.Retries),
	}
	go w.flushAll()
	return w
}

func (w *dataWriter) flushAll() {
	for {
		w.Lock()
		now := time.Now()
		for _, f := range w.files {
			f.refs++
			w.Unlock()
			tooMany := f.totalSlices() > 800
			f.Lock()

			lastBit := uint32(rand.Int() % 2) // choose half of chunks randomly
			for i, c := range f.chunks {
				hs := len(c.slices) / 2
				for j, s := range c.slices {
					if !s.freezed && (now.Sub(s.started) > flushDuration || now.Sub(s.lastMod) > time.Second && now.Sub(s.started) > time.Second ||
						tooMany && i%2 == lastBit && j <= hs) {
						s.freezed = true
						go s.flushData()
					}
				}
			}
			f.Unlock()
			w.free(f)
			w.Lock()
		}
		w.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (w *dataWriter) Open(inode Ino, len uint64, tierID uint8) FileWriter {
	w.Lock()
	defer w.Unlock()
	f, ok := w.files[inode]
	if !ok {
		f = &fileWriter{
			w:      w,
			inode:  inode,
			length: len,
			tierID: tierID,
			chunks: make(map[uint32]*chunkWriter),
		}
		f.flushcond = utils.NewCond(f)
		f.writecond = utils.NewCond(f)
		f.commitcond = utils.NewCond(f)
		w.files[inode] = f
	}
	f.refs++
	return f
}

func (w *dataWriter) find(inode Ino) *fileWriter {
	w.Lock()
	defer w.Unlock()
	return w.files[inode]
}

func (w *dataWriter) free(f *fileWriter) {
	w.Lock()
	defer w.Unlock()
	f.refs--
	if f.refs == 0 {
		delete(w.files, f.inode)
	}
}

func (w *dataWriter) Flush(ctx meta.Context, inode Ino) syscall.Errno {
	f := w.find(inode)
	if f != nil {
		return f.Flush(ctx)
	}
	// No writer state left for this inode, but the drive9 HTTP meta commits
	// slices asynchronously (QueueWriteParts): a commit can still be in
	// flight after the fileWriter was freed, so a reader/truncate would see a
	// length and slice list older than a write the kernel already completed.
	// JuiceFS's synchronous Meta.Write made that impossible; drain the
	// per-inode queue instead of returning early.
	if m, ok := w.m.(metaWaitWrites); ok {
		return m.WaitWrites(inode)
	}
	return 0
}

func (w *dataWriter) GetLength(inode Ino) uint64 {
	f := w.find(inode)
	if f != nil {
		return f.GetLength()
	}
	return 0
}

func (w *dataWriter) Truncate(inode Ino, len uint64) {
	f := w.find(inode)
	if f != nil {
		f.Truncate(len)
	}
}

func (w *dataWriter) UpdateMtime(inode Ino, mtime time.Time) {
	f := w.find(inode)
	if f != nil {
		f.updateMtime(mtime)
	}
}

func (w *dataWriter) FlushAll() error {
	var err error
	w.Lock()
	for inode, ind := range w.files {
		ind.refs++
		w.Unlock()
		eno := ind.Flush(meta.Background())
		w.free(ind)
		if eno != 0 {
			logger.Errorf("flush %s: %s", inode, eno)
			return eno
		}
		logger.Debugf("Flush %d", inode)
		w.Lock()
	}
	w.Unlock()
	return err
}
