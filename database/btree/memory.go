package btree

import (
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const (
	KiB float64 = 1 << (10 * iota)
	MiB
	GiB
	TiB

	KB = 1000
	MB = KB * 1000
	GB = MB * 1000
	TB = GB * 1000
)

// Represents computer memory
type Memory uint64

func (m Memory) Bytes() uint64 {
	return uint64(m)
}

func (m Memory) KiB() float64 {
	return float64(m) / KiB
}

func (m Memory) MiB() float64 {
	return float64(m) / MiB
}

func (m Memory) GiB() float64 {
	return float64(m) / GiB
}

func (m Memory) TiB() float64 {
	return float64(m) / TiB
}

func (m Memory) KB() float64 {
	return float64(m) / KB
}

func (m Memory) MB() float64 {
	return float64(m) / MB
}

func (m Memory) GB() float64 {
	return float64(m) / GB
}

func (m Memory) TB() float64 {
	return float64(m) / TB
}

func (m Memory) String() string {
	p := message.NewPrinter(language.English)
	return p.Sprintf("%d Bytes", m.Bytes())
}
