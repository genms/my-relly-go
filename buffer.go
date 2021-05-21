package main

import "errors"

var (
	ErrIo           = errors.New("I/O error")
	ErrNoFreeBuffer = errors.New("no free buffer available in buffer pool")
)

type BufferId int

type Buffer struct {
	PageId   PageId
	Page     [PAGE_SIZE]byte
	IsDirty  bool
	Bollowed bool
}

type Frame struct {
	usageCount int
	refCount   int
	buffer     Buffer
}

type BufferPool struct {
	buffers      []Frame
	nextVictimId BufferId
}

func NewBufferPool(poolSize int) *BufferPool {
	bufferPool := BufferPool{}
	bufferPool.buffers = make([]Frame, poolSize)
	return &bufferPool
}

func (p *BufferPool) size() int {
	return len(p.buffers)
}

func (p *BufferPool) evict() (BufferId, error) {
	poolSize := p.size()
	consecutivePinned := 0

	var ret BufferId
	for {
		nextVictimId := p.nextVictimId
		frame := &p.buffers[nextVictimId]
		if frame.usageCount == 0 {
			ret = p.nextVictimId
			break
		}
		if frame.refCount == 0 {
			frame.usageCount--
			consecutivePinned = 0
		} else {
			consecutivePinned++
			if consecutivePinned >= poolSize {
				return -1, ErrNoFreeBuffer
			}
		}
		p.nextVictimId = p.incrementId(p.nextVictimId)
	}
	return ret, nil
}

func (p *BufferPool) incrementId(bufferId BufferId) BufferId {
	return BufferId((int(bufferId) + 1) % p.size())
}

type BufferPoolManager struct {
	disk      *DiskManager
	pool      *BufferPool
	pageTable map[PageId]BufferId
}

func NewBufferPoolManager(disk *DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk,
		pool,
		map[PageId]BufferId{},
	}
}

func (m *BufferPoolManager) FetchPage(pageId PageId) (*Buffer, error) {
	if bufferId, ok := m.pageTable[pageId]; ok {
		frame := &m.pool.buffers[bufferId]
		frame.usageCount++
		frame.refCount++
		return &frame.buffer, nil
	}
	bufferId, err := m.pool.evict()
	if err != nil {
		return nil, err
	}
	frame := &m.pool.buffers[bufferId]
	evictPageId := frame.buffer.PageId

	buffer := &frame.buffer
	if buffer.IsDirty {
		err = m.disk.WritePageData(evictPageId, buffer.Page[:])
		if err != nil {
			return nil, err
		}
	}
	buffer.PageId = pageId
	buffer.IsDirty = false
	err = m.disk.ReadPageData(pageId, buffer.Page[:])
	if err != nil {
		return nil, err
	}
	frame.usageCount = 1
	frame.refCount = 1

	delete(m.pageTable, evictPageId)
	m.pageTable[pageId] = bufferId
	return buffer, nil
}

func (m *BufferPoolManager) CreatePage() (*Buffer, error) {
	bufferId, err := m.pool.evict()
	if err != nil {
		return nil, err
	}
	frame := &m.pool.buffers[bufferId]
	evictPageId := frame.buffer.PageId

	buffer := &frame.buffer
	if buffer.IsDirty {
		err = m.disk.WritePageData(evictPageId, buffer.Page[:])
		if err != nil {
			return nil, err
		}
	}

	pageId := m.disk.AllocatePage()
	*buffer = Buffer{PageId: pageId, IsDirty: true}
	frame.usageCount = 1
	frame.refCount = 1

	delete(m.pageTable, evictPageId)
	m.pageTable[pageId] = bufferId
	return buffer, nil
}

func (m *BufferPoolManager) FinishUsingPage(buffer *Buffer) {
	bufferId, ok := m.pageTable[buffer.PageId]
	if !ok {
		panic("Not exist in page table")
	}

	frame := &m.pool.buffers[bufferId]
	if frame.refCount == 0 {
		panic("Can't release any more")
	}
	frame.refCount--
}

func (m *BufferPoolManager) Flush() error {
	for pageId, bufferId := range m.pageTable {
		frame := &m.pool.buffers[bufferId]
		page := &frame.buffer.Page
		err := m.disk.WritePageData(pageId, page[:])
		if err != nil {
			return err
		}
		frame.buffer.IsDirty = false
	}
	m.disk.Sync()
	return nil
}
