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

package chunk

import (
	"context"
	"io"

	"github.com/juicedata/juicefs/pkg/object"
)

type Reader interface {
	ReadAt(ctx context.Context, p *Page, off int) (int, error)
}

type Writer interface {
	io.WriterAt
	ID() uint64
	SetID(id uint64)
	SetWriteback(enabled bool)
	FlushTo(offset int) error
	Finish(length int) error
	Abort()
}

// BufferedWriter is implemented by writers that keep written blocks in memory
// until Finish hands them to the object store. A reader can use it to serve
// read-your-writes from the write buffer, which matters when finishing a block
// is expensive (an HTTP metadata commit) or seals it against further writes.
type BufferedWriter interface {
	// BufferedStart returns the offset below which data has already been
	// uploaded and its memory released, so the buffer can no longer answer
	// for it even though the slice may not be visible in metadata yet.
	BufferedStart() int
	// ReadBuffered copies up to len(p) bytes starting at off, both relative
	// to the start of this slice, and returns how many bytes were copied. It
	// stops at the first byte that is no longer in memory.
	ReadBuffered(p []byte, off int) int
}

type ChunkStore interface {
	NewReader(id uint64, length int) Reader
	NewWriter(id uint64, tierID uint8) Writer
	Remove(id uint64, length int) error
	FillCache(id uint64, length uint32) error
	EvictCache(id uint64, length uint32) error
	CheckCache(id uint64, length uint32, handler func(exists bool, loc string, size int)) error
	UsedMemory() int64
	UpdateLimit(upload, download int64)
	BlobStorage() object.ObjectStorage
}
