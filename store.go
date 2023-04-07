package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

// 10 MB
const MAX_LOG_FILE_BYTES = 1680

type LogStoreOptions struct {
	maxLogFileBytes int
}

type Store struct {
	logDir       string
	mp           *mp[Item]
	logFile      *os.File
	logFileMutex *sync.Mutex
	opts         *LogStoreOptions
}

type Item struct {
	file      string
	expire    int
	valuePos  int
	valueSize int
}

// Get a value from disk
func (store *Store) Get(key string, w io.Writer) (bool, error) {
	access := store.mp.AccessShard(key)
	defer access.Unlock()

	item, found := store.mp.GetKey(key)

	if !found {
		return false, nil
	} else if int(time.Now().UnixMilli()) >= item.expire {
		store.mp.DeleteKey(key)
		return false, nil
	}

	f, err := os.Open(item.file)
	if err != nil {
		return false, fmt.Errorf("couldn't open log file %s: %s", store.logFile.Name(), err)
	}
	defer f.Close()

	// seek to the value pos
	_, err = f.Seek(int64(item.valuePos), 0)
	if err != nil {
		return false, fmt.Errorf("couldn't seek in %s: %s", store.logFile.Name(), err)
	}

	_, err = io.CopyN(w, f, int64(item.valueSize))
	if err != nil {
		return true, err
	}
	return true, nil
}

// sets a value
func (store *Store) Set(key string, expire int, data []byte) error {
	access := store.mp.AccessShard(key)
	defer access.Unlock()
	store.logFileMutex.Lock()
	defer store.logFileMutex.Unlock()
	fi, err := store.logFile.Stat()
	if err != nil {
		return fmt.Errorf("couldn't stat log file %s: %s", store.logFile.Name(), err)
	}
	end := int(fi.Size())

	line := []byte(fmt.Sprintf("%d,%d,%d,%s,", expire, len(key), len(data), key))
	lineLength := len(line) + len(data)

	if end+lineLength >= store.opts.maxLogFileBytes {
		err := store.nextLogFile()
		if err != nil {
			return err
		}

		end = 0
	}

	val := append(append(line, data...), ","...)
	_, err = store.logFile.Write(val)
	if err != nil {
		return fmt.Errorf("couldn't write to %s: %s", store.logFile.Name(), err)
	}

	item := Item{
		store.logFile.Name(),
		expire,
		end + len(line),
		len(data),
	}

	if int(time.Now().UnixMilli()) >= expire {
		store.mp.DeleteKey(string(key))
		return nil
	} else {
		store.mp.SetKey(key, item)
	}

	return nil

}

// nextLogFile rolls the LogStore's log file to a newly created file
func (logStore *Store) nextLogFile() error {
	defer logStore.logFile.Close()
	logFile, err := createLogFile(logStore.logDir)
	if err != nil {
		return err
	}
	logStore.logFile = logFile
	return nil
}

// disk storage format expire, keysize,valuesize, key, value
// expire is 4 bytes
// keysize is 4 bytes
// valuesize is 4 bytes
// parse a log file
func parseLogFile(path string) (map[string]Item, error) {
	const COMMA byte = 44
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't open log file %s : %s", path, err)
	}
	defer f.Close()
	items := make(map[string]Item)
	r := bufio.NewReader(f)
	curr := 0
	for {
		_expire, err := r.ReadBytes(COMMA)
		if err == io.EOF {
			break
		}
		curr += len(_expire)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse expire %s ", err)
		}
		// convert byte to int
		expire, err := strconv.Atoi(string(_expire[:len(_expire)-1]))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse expire %s ", err)
		}

		// read keysize
		_keysize, err := r.ReadBytes(COMMA)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse keysize %s ", err)
		}
		// convert byte to int
		keysize, err := strconv.Atoi(string(_keysize[:len(_keysize)-1]))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse expire %s ", err)
		}
		curr += len(_keysize)

		// read keysize
		_valuesize, err := r.ReadBytes(COMMA)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse keysize %s ", err)
		}
		// convert byte to int
		valuesize, err := strconv.Atoi(string(_valuesize[:len(_valuesize)-1]))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse expire %s ", err)
		}
		curr += len(_valuesize)

		// read keys
		keybuffer := make([]byte, keysize+1)
		n, err := r.Read(keybuffer)
		curr += n

		if err != nil {
			return nil, fmt.Errorf("couldn't parse key: %s", err)
		}
		key := string(keybuffer[:keysize])
		valueOffset := curr

		n, err = r.Discard(valuesize + 1) // Skip value (+ 1 for the comma between metadata)
		curr += n
		if err != nil {
			return nil, fmt.Errorf("during key (%s) couldn't skip value: %s", key, err)
		}

		if int(time.Now().UnixMilli()) < expire {
			items[key] = Item{
				path,
				expire,
				valueOffset,
				valuesize,
			}
		} else {
			delete(items, string(keybuffer))
		}
	}
	return items, nil
}

// create log store
func CreateLogStore(logDir string, opts *LogStoreOptions) (*Store, error) {
	if opts == nil {
		opts = &LogStoreOptions{maxLogFileBytes: MAX_LOG_FILE_BYTES}
	}
	// creates directory chain
	err := os.MkdirAll(logDir, os.ModePerm)

	if err != nil {
		return nil, fmt.Errorf("couldn't create directory %s: %s", logDir, err)
	}

	logFiles, err := ioutil.ReadDir(logDir)

	if err != nil {
		return nil, fmt.Errorf("couldn't read directory %s: %s", logDir, err)
	}

	//load all keys into map
	sort.Slice(logFiles, func(i int, j int) bool {
		return logFiles[i].Name() < logFiles[j].Name()
	})

	concurrentMap := CreateMap[Item]()

	for _, fileInfo := range logFiles {
		keys, err := parseLogFile(path.Join(logDir, fileInfo.Name()))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse log file %s: %s", path.Join(logDir, fileInfo.Name()), err)
		}
		concurrentMap.MSet(keys)
	}

	var logFile *os.File
	// already some log files present
	if len(logFiles) > 0 {
		latest := logFiles[len(logFiles)-1]
		latestPath := path.Join(logDir, latest.Name())
		fi, err := os.Stat(latestPath)
		if err != nil {
			return nil, fmt.Errorf("couldn't stat log file %s: %s", path.Join(logDir, latestPath), err)
		}

		if fi.Size() >= int64(opts.maxLogFileBytes) {
			// If the latest file on disk is reaching capacity
			logFile, err = createLogFile(logDir)
			if err != nil {
				return nil, err
			}
		} else {
			// otherwise open the file
			logFile, err = os.OpenFile(latestPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// If this is a new directory, create the first log file
		logFile, err = createLogFile(logDir)
		if err != nil {
			return nil, err
		}
	}

	return &Store{
		logDir,
		concurrentMap,
		logFile,
		&sync.Mutex{},
		opts,
	}, nil

}

func createLogFile(logDir string) (*os.File, error) {
	id := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), rndFileString(16))
	logFile, err := os.Create(path.Join(logDir, id))
	if err != nil {
		return nil, fmt.Errorf("couldn't create log file %s: %s", path.Join(logDir, id), err)
	}
	return logFile, nil
}

// rndFileString returns a random string appropriate for a filename
func rndFileString(length int) []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}
