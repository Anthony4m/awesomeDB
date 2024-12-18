package kfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

type Page struct {
	data   []byte
	pageId uint64
	mu     sync.RWMutex
}

const (
	ErrOutOfBounds = "offset out of bounds"
)

const pageIdOffset = 0

func NewPage(blockSize int) *Page {
	page := &Page{
		data: make([]byte, blockSize),
	}
	return page
}

func NewPageFromBytes(b []byte) *Page {
	page := &Page{
		data: b,
	}
	return page
}

func (p *Page) GetInt(offset int) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset > len(p.data) {
		return 0, fmt.Errorf("%s: getting int", ErrOutOfBounds)
	}
	return int(binary.BigEndian.Uint32(p.data[offset:])), nil
}

func (p *Page) SetInt(offset int, val int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+4 > len(p.data) {
		return fmt.Errorf("%s: setting int", ErrOutOfBounds)
	}
	binary.BigEndian.PutUint32(p.data[offset:], uint32(val))
	return nil
}

func (p *Page) GetBytes(offset int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset > len(p.data) {
		return nil, fmt.Errorf("%s: getting bytes", ErrOutOfBounds)
	}

	end := offset
	for end < len(p.data) && p.data[end] != 0 {
		end++
	}

	dataCopy := make([]byte, end-offset)
	copy(dataCopy, p.data[offset:end])

	return dataCopy, nil
}

func (p *Page) GetBytesWithLen(offset int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset > len(p.data) {
		return nil, fmt.Errorf("%s: getting bytes", ErrOutOfBounds)
	}

	end := offset
	for end < len(p.data) && p.data[end] != 0 {
		end++
	}

	dataCopy := make([]byte, end+int(unsafe.Sizeof(0))-offset)
	copy(dataCopy, p.data[offset:end+int(unsafe.Sizeof(0))])

	return dataCopy, nil
}

func (p *Page) SetBytes(offset int, val []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	length := len(val)
	if length != 0 {
		if offset+length+1 > len(p.data) {
			return fmt.Errorf("%s: setting bytes", ErrOutOfBounds)
		}

		for i := 0; i < length+1; i++ {
			p.data[offset+i] = 0
		}

		copy(p.data[offset:], val)

		p.data[offset+length] = 0
	}

	return nil
}

func (p *Page) GetString(offset int) (string, error) {
	if offset > len(p.data) {
		return "", fmt.Errorf("%s: getting string", ErrOutOfBounds)
	}

	b, err := p.GetBytes(offset)
	if err != nil {
		return "", fmt.Errorf("error occured %s", err)
	}

	str := string(b)
	return str, nil
}

func (p *Page) GetStringWithOffset(offset int) (string, error) {
	if offset > len(p.data) {
		return "", fmt.Errorf("%s: getting string", ErrOutOfBounds)
	}

	b, err := p.GetBytesWithLen(offset)
	if err != nil {
		return "", fmt.Errorf("error occurred %s", err)
	}

	// Check if there are at least 4 bytes for the offset
	if len(b) < 4 {
		return "", fmt.Errorf("insufficient bytes to extract string")
	}
	stringBytes := b[:len(b)-4]
	trimmedBytes := bytes.TrimRight(stringBytes, "\x00")

	str := string(trimmedBytes)
	return str, nil
}

func (p *Page) SetString(offset int, val string) error {
	strBytes := append([]byte(val))

	p.SetBytes(offset, strBytes)
	return nil
}

func (p *Page) SetBool(offset int, val bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+1 > len(p.data) {
		return fmt.Errorf("%s: setting bool", ErrOutOfBounds)
	}
	if val {
		p.data[offset] = 1
	} else {
		p.data[offset] = 0
	}
	return nil
}

func (p *Page) GetBool(offset int) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+1 > len(p.data) {
		return false, fmt.Errorf("%s: getting bool", ErrOutOfBounds)
	}
	if p.data[offset] == 1 {
		return true, nil
	}
	return false, nil
}

func (p *Page) SetDate(offset int, val time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+8 > len(p.data) {
		return fmt.Errorf("%s: setting date", ErrOutOfBounds)
	}
	convertedVal := uint64(val.Unix())
	binary.BigEndian.PutUint64(p.data[offset:], convertedVal)
	return nil
}

func (p *Page) GetDate(offset int) (time.Time, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset+8 > len(p.data) {
		return time.Unix(0, 0), fmt.Errorf("%s: getting date", ErrOutOfBounds)
	}
	timestamp := binary.BigEndian.Uint64(p.data[offset:])
	return time.Unix(int64(timestamp), 0), nil
}

func (p *Page) Contents() []byte {
	return p.data
}

func trimZero(s []byte) []byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0 {
			return s[:i+1]
		}
	}
	return []byte{}
}
