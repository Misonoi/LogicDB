package bitcask

import (
	"encoding/binary"
	"fmt"
	bloom "github.com/liyue201/gostl/ds/bloomfilter"
	"hash/crc32"
	"io"
	"logicdb"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)
import "github.com/chen3feng/stl4go"

const (
	MaxFileSize uint64 = 1024 * 1024 * 1024
	DefaultDir         = "./models/bitcask/"
)

type InternalKey struct {
	key []byte
}

type InternalValue struct {
	gen       uint64
	valueSize uint64
	pos       uint64
	timestamp uint64
}

func ValueWithInternal(gen uint64, valueSize uint64, pos uint64, timestamp uint64) InternalValue {
	return InternalValue{
		gen:       gen,
		valueSize: valueSize,
		pos:       pos,
		timestamp: timestamp,
	}
}

func InternalKeyCP(a, b *InternalKey) int {
	if len(a.key) > len(b.key) {
		return 1
	} else if len(a.key) < len(b.key) {
		return -1
	}

	return strings.Compare(string(a.key), string(b.key))
}

type EntryMeta struct {
	crc       uint32
	timestamp uint64
	keySize   uint64
	valueSize uint64
}

func newEntry(key []byte, value []byte, deleted bool) *Entry {
	entryMeta := &EntryMeta{
		crc:       0,
		timestamp: uint64(time.Now().Unix()),
		keySize:   uint64(len(key)),
		valueSize: logicdb.IF(deleted == false, uint64(len(value)), 0),
	}

	entryMeta.crc = Crc32(entryMeta.timestamp, entryMeta.keySize, entryMeta.valueSize,
		key, value)

	return &Entry{
		meta:  entryMeta,
		key:   key,
		value: value,
	}
}

func Crc32(timestamp uint64, keySize uint64, valueSize uint64, key []byte, value []byte) uint32 {
	crc := crc32.NewIEEE()

	all := make([]byte, 24)

	binary.LittleEndian.PutUint64(all, timestamp)
	binary.LittleEndian.PutUint64(all[8:], keySize)
	binary.LittleEndian.PutUint64(all[16:], valueSize)

	_, _ = crc.Write(all)
	_, _ = crc.Write(key)
	_, _ = crc.Write(value)

	return crc.Sum32()
}

func metaToByteSlice(data *EntryMeta) []byte {
	buffer := make([]byte, binary.Size(*data))

	binary.LittleEndian.PutUint32(buffer, data.crc)
	binary.LittleEndian.PutUint64(buffer[4:], data.timestamp)
	binary.LittleEndian.PutUint64(buffer[12:], data.keySize)
	binary.LittleEndian.PutUint64(buffer[20:], data.valueSize)

	return buffer
}

type Entry struct {
	meta  *EntryMeta
	key   []byte
	value []byte
}

func entryToByteSlice(entry *Entry) []byte {
	metaSize := binary.Size(entry.meta)
	buffer := make([]byte, binary.Size(entry.meta)+len(entry.key)+len(entry.value))

	copy(buffer[0:], metaToByteSlice(entry.meta))
	copy(buffer[metaSize:], entry.key)
	copy(buffer[metaSize+len(entry.key):], entry.value)

	return buffer
}

type BitCask struct {
	dir        *os.File
	currentGen uint64
	current    *logicdb.WriterWithPos[*os.File]
	mem        *stl4go.SkipList[*InternalKey, InternalValue]
	fs         map[uint64]*logicdb.ReaderWithPos[*os.File]
	filter     *bloom.BloomFilter
	config     *Config
}

func KeyWithInternal(key []byte) *InternalKey {
	return &InternalKey{key: key}
}

func (b *BitCask) Get(key []byte) ([]byte, error) {
	if !b.filter.Contains(string(key)) {
		return nil, logicdb.WrapKeyNotFoundErr(key)
	}

	value := b.mem.Find(KeyWithInternal(key))

	if value == nil {
		return nil, logicdb.WrapKeyNotFoundErr(key)
	}

	f := b.fs[value.gen]

	res := make([]byte, value.valueSize)
	_, err := f.ReadAt(res, int64(value.pos))

	if err != nil {
		return nil, err
	}

	return res, nil
}

func writeEntry(f *logicdb.WriterWithPos[*os.File], entry *Entry) (int, error) {
	return f.Write(entryToByteSlice(entry))
}

func (b *BitCask) switchCurrent() error {
	logicdb.AllocGen()
	gen := atomic.LoadUint64(&logicdb.GenBuf)

	file, err := os.Create(filepath.Join(b.config.Dir, fmt.Sprintf("%v.bc", gen)))

	if err != nil {
		return err
	}

	b.currentGen = gen
	b.current = logicdb.NewWriterWithPos(file)
	return nil
}

func (b *BitCask) writeEntry(entry *Entry) (int, error) {
	return writeEntry(b.current, entry)
}

func (b *BitCask) Set(key []byte, value []byte) error {
	entry := newEntry(key, value, false)
	_, err := b.writeEntry(entry)

	if err != nil {
		return err
	}

	b.filter.Add(string(key))

	b.mem.Insert(KeyWithInternal(key),
		ValueWithInternal(b.currentGen, entry.meta.valueSize, b.current.Pos-entry.meta.valueSize, entry.meta.timestamp))

	println(b.current.Pos)

	if b.current.Pos >= MaxFileSize {
		return b.switchCurrent()
	}

	return nil
}

func (b *BitCask) Remove(key []byte) (bool, error) {
	if !b.filter.Contains(string(key)) {
		return false, nil
	}

	if !b.mem.Has(KeyWithInternal(key)) {
		return false, nil
	}

	entry := newEntry(key, make([]byte, 0), true)
	_, err := b.writeEntry(entry)

	if err != nil {
		return false, err
	}

	b.mem.Remove(KeyWithInternal(key))

	return true, nil
}

func (b *BitCask) RemoveWithGet(key []byte) ([]byte, bool, error) {
	if !b.filter.Contains(string(key)) {
		return nil, false, nil
	}

	if !b.mem.Has(KeyWithInternal(key)) {
		return nil, false, nil
	}

	res, err := b.Get(key)

	if err != nil {
		return nil, false, err
	}

	ok, err := b.Remove(key)

	if err != nil {
		return nil, false, err
	}

	return res, ok, nil
}

func (b *BitCask) Contains(key []byte) (bool, error) {
	if !b.filter.Contains(string(key)) {
		return false, nil
	}

	return b.mem.Has(KeyWithInternal(key)), nil
}

func readEntry(f *logicdb.ReaderWithPos[*os.File]) (*Entry, error) {
	entry := &Entry{
		meta:  nil,
		key:   nil,
		value: nil,
	}

	meta, err := readEntryMeta(f)

	if err != nil {
		return nil, err
	}

	keyValue := make([]byte, meta.keySize+meta.valueSize)

	_, err = f.Read(keyValue)

	if err != nil {
		return nil, err
	}

	entry.meta = meta
	entry.key = keyValue[0:meta.keySize]
	entry.value = keyValue[meta.keySize:]

	return entry, nil
}

func readEntryMeta(f *logicdb.ReaderWithPos[*os.File]) (*EntryMeta, error) {
	meta := &EntryMeta{
		crc:       0,
		timestamp: 0,
		keySize:   0,
		valueSize: 0,
	}

	all := make([]byte, 4+8*3)

	_, err := f.Read(all)

	if err != nil {
		return nil, err
	}

	meta.crc = binary.LittleEndian.Uint32(all)
	meta.timestamp = binary.LittleEndian.Uint64(all[4:])
	meta.keySize = binary.LittleEndian.Uint64(all[12:])
	meta.valueSize = binary.LittleEndian.Uint64(all[20:])

	return meta, nil
}

func (b *BitCask) recoverSingle(f *os.File, gen uint64) error {
	fs := logicdb.NewReaderWithPos(f)
	b.fs[gen] = fs

	for entry, err := readEntry(fs); err == nil; entry, err = readEntry(fs) {
		if entry.meta.valueSize != 0 {
			b.filter.Add(string(entry.key))

			b.mem.Insert(KeyWithInternal(entry.key),
				ValueWithInternal(gen, entry.meta.valueSize, fs.Pos-entry.meta.valueSize, entry.meta.timestamp))
		} else {
			b.filter.Add(string(entry.key))

			b.mem.Remove(KeyWithInternal(entry.key))
		}
	}

	_, err := fs.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	return nil
}

func genPath(gen uint64, dir string) string {
	return filepath.Join(dir, fmt.Sprintf("%v.bc", gen))
}

func (b *BitCask) recover() error {
	gens, err := sortedGen(b)

	if err != nil {
		return err
	}

	for idx, e := range gens {
		file, err := os.OpenFile(genPath(e, b.config.Dir),
			os.O_RDWR|os.O_APPEND, os.ModePerm)

		if err != nil {
			return err
		}

		err = b.recoverSingle(file, e)

		if err != nil {
			return err
		}

		if idx == len(gens)-1 {
			b.current = logicdb.NewWriterWithPos(file)
			b.currentGen = e
		}
	}

	if len(gens) == 0 {
		logicdb.AllocGen()
		gen := atomic.LoadUint64(&logicdb.GenBuf)

		file, err := os.Create(genPath(gen, b.config.Dir))

		if err != nil {
			return err
		}

		b.current = logicdb.NewWriterWithPos(file)
		b.currentGen = gen
		b.fs[gen] = logicdb.NewReaderWithPos(file)
	}

	return nil
}

type BKernel struct {
	bitCask *BitCask
	config  *Config
}

func KernelWithConfig(config *Config) *BKernel {
	return &BKernel{
		bitCask: nil,
		config:  config,
	}
}

func open(config *Config) (*BitCask, error) {
	err := os.MkdirAll(config.Dir, os.ModePerm)

	if err != nil {
		return nil, err
	}

	dir, err := os.Open(config.Dir)

	if err != nil {
		return nil, err
	}

	bitCask := &BitCask{
		dir:        dir,
		current:    nil,
		currentGen: 0,
		mem:        stl4go.NewSkipListFunc[*InternalKey, InternalValue](InternalKeyCP),
		config:     config,
		fs:         make(map[uint64]*logicdb.ReaderWithPos[*os.File]),
		filter:     bloom.New(100, 4, bloom.WithGoroutineSafe()),
	}

	err = bitCask.recover()

	if err != nil {
		return nil, err
	}

	return bitCask, nil
}

func sortedGen(bitCask *BitCask) ([]uint64, error) {
	dir := bitCask.dir

	entries, err := os.ReadDir(dir.Name())

	if err != nil {
		return nil, err
	}

	res := make([]uint64, 0)

	for _, file := range entries {
		name := filepath.Base(file.Name())
		ext := filepath.Ext(name)

		nameWithoutExt := strings.TrimSuffix(name, ext)

		if ext == ".bc" {
			gen, err := strconv.ParseUint(nameWithoutExt, 10, 64)

			if err != nil {
				return nil, err
			}

			res = append(res, uint64(gen))
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})

	return res, nil
}

type Config struct {
	MaxFileSize uint64
	Dir         string
}

func NewBitCaskConfigWithDefault() *Config {
	return &Config{
		MaxFileSize: MaxFileSize,
		Dir:         DefaultDir,
	}
}

func (k *BKernel) Open(path string) error {
	if path != "" {
		k.config.Dir = path
	}

	bitCask, err := open(k.config)

	if err != nil {
		return err
	}

	k.bitCask = bitCask
	return nil
}

func (k *BKernel) Get(key []byte) ([]byte, error) {
	return k.bitCask.Get(key)
}

func (k *BKernel) Set(key []byte, value []byte) error {
	return k.bitCask.Set(key, value)
}

func (k *BKernel) Remove(key []byte) (bool, error) {
	return k.bitCask.Remove(key)
}

func (k *BKernel) RemoveWithGet(key []byte) ([]byte, bool, error) {
	return k.bitCask.RemoveWithGet(key)
}

func (k *BKernel) Contains(key []byte) (bool, error) {
	return k.bitCask.Contains(key)
}