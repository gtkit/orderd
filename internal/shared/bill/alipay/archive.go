package alipay

import (
	"encoding/binary"
	"io"

	"github.com/gtkit/orderd/internal/shared/bill/billio"
)

const (
	maxArchiveFiles      = 8
	maxDirectoryBytes    = 1 << 20
	endHeaderBytes       = 22
	maxArchiveTail       = endHeaderBytes + 65535
	directoryHeaderBytes = 46
)

// checkArchive bounds allocation before archive/zip reads the central directory.
// Offsets follow the same classic ZIP headers as archive/zip/reader.go. Only small,
// single-disk, non-ZIP64 archives are supported; the standard reader still verifies
// member headers, decompression and CRC. Names remain opaque and are never extracted.
func checkArchive(r io.ReaderAt, size int64) error {
	if size < endHeaderBytes || size > billio.MaxDownloadBytes {
		return ErrInvalidBill
	}
	tail := make([]byte, min(size, maxArchiveTail))
	if _, err := r.ReadAt(tail, size-int64(len(tail))); err != nil {
		return ErrInvalidBill
	}
	end := -1
	for i := len(tail) - endHeaderBytes; i >= 0; i-- {
		if binary.LittleEndian.Uint32(tail[i:]) == 0x06054b50 &&
			i+endHeaderBytes+int(binary.LittleEndian.Uint16(tail[i+20:])) == len(tail) {
			end = i
			break
		}
	}
	if end < 0 {
		return ErrInvalidBill
	}
	e := tail[end:]
	count := binary.LittleEndian.Uint16(e[10:])
	directorySize := binary.LittleEndian.Uint32(e[12:])
	directoryOffset := binary.LittleEndian.Uint32(e[16:])
	endOffset := size - int64(len(tail)) + int64(end)
	if binary.LittleEndian.Uint16(e[4:]) != 0 || binary.LittleEndian.Uint16(e[6:]) != 0 ||
		count == 0 || count > maxArchiveFiles || binary.LittleEndian.Uint16(e[8:]) != count ||
		directorySize == 0 || directorySize > maxDirectoryBytes || directoryOffset == ^uint32(0) ||
		int64(directoryOffset)+int64(directorySize) != endOffset {
		return ErrInvalidBill
	}
	// A ZIP64 locator can occur even when the classic fields are not sentinels.
	if endOffset >= 20 {
		var locator [4]byte
		if _, err := r.ReadAt(locator[:], endOffset-20); err != nil || binary.LittleEndian.Uint32(locator[:]) == 0x07064b50 {
			return ErrInvalidBill
		}
	}
	directory := make([]byte, directorySize)
	if _, err := r.ReadAt(directory, int64(directoryOffset)); err != nil {
		return ErrInvalidBill
	}
	var actual int
	for len(directory) > 0 {
		if actual >= maxArchiveFiles || len(directory) < directoryHeaderBytes || binary.LittleEndian.Uint32(directory) != 0x02014b50 {
			return ErrInvalidBill
		}
		nameSize := int(binary.LittleEndian.Uint16(directory[28:]))
		extraSize := int(binary.LittleEndian.Uint16(directory[30:]))
		commentSize := int(binary.LittleEndian.Uint16(directory[32:]))
		recordSize := directoryHeaderBytes + nameSize + extraSize + commentSize
		if recordSize > len(directory) || nameSize == 0 || binary.LittleEndian.Uint16(directory[34:]) != 0 ||
			binary.LittleEndian.Uint32(directory[20:]) == ^uint32(0) || binary.LittleEndian.Uint32(directory[24:]) == ^uint32(0) ||
			binary.LittleEndian.Uint32(directory[42:]) >= directoryOffset {
			return ErrInvalidBill
		}
		extra := directory[directoryHeaderBytes+nameSize : directoryHeaderBytes+nameSize+extraSize]
		for len(extra) > 0 {
			if len(extra) < 4 || binary.LittleEndian.Uint16(extra) == 0x0001 {
				return ErrInvalidBill
			}
			n := 4 + int(binary.LittleEndian.Uint16(extra[2:]))
			if n > len(extra) {
				return ErrInvalidBill
			}
			extra = extra[n:]
		}
		directory = directory[recordSize:]
		actual++
	}
	if actual != int(count) {
		return ErrInvalidBill
	}
	return nil
}
