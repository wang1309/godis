package database

import (
	"godis/datastruct/dict"
	"godis/datastruct/lock"
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/lib/timewheel"
	"sync"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// DB stores data and execute user's commands
type DB struct {
	index int
	// key -> DataEntity
	data dict.Dict
	// key -> expireTime (time.Time)
	ttlMap dict.Dict
	// key -> version(uint32)
	versionMap dict.Dict

	// dict.Dict will ensure concurrent-safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	locker *lock.Locks
	// stop all data access for execFlushDB
	stopWorld sync.WaitGroup
	addAof    func(CmdLine)
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker:     lock.Make(lockerSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}


// makeBasicDB create DB instance only with basic abilities.
// It is not concurrent safe
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeSimple(),
		ttlMap:     dict.MakeSimple(),
		versionMap: dict.MakeSimple(),
		locker:     lock.Make(1),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
//func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
//	// transaction control commands and other commands which cannot execute within transaction
//	cmdName := strings.ToLower(string(cmdLine[0]))
//	if cmdName == "multi" {
//		if len(cmdLine) != 1 {
//			return protocol.MakeArgNumErrReply(cmdName)
//		}
//		return StartMulti(c)
//	} else if cmdName == "discard" {
//		if len(cmdLine) != 1 {
//			return protocol.MakeArgNumErrReply(cmdName)
//		}
//		return DiscardMulti(c)
//	} else if cmdName == "exec" {
//		if len(cmdLine) != 1 {
//			return protocol.MakeArgNumErrReply(cmdName)
//		}
//		return execMulti(db, c)
//	} else if cmdName == "watch" {
//		if !validateArity(-2, cmdLine) {
//			return protocol.MakeArgNumErrReply(cmdName)
//		}
//		return Watch(db, c, cmdLine[1:])
//	} else if cmdName == "flushdb" {
//		if !validateArity(1, cmdLine) {
//			return protocol.MakeArgNumErrReply(cmdName)
//		}
//		if c.InMultiState() {
//			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
//		}
//		return execFlushDB(db, cmdLine[1:])
//	}
//	if c != nil && c.InMultiState() {
//		EnqueueCmd(c, cmdLine)
//		return protocol.MakeQueuedReply()
//	}
//
//	return db.execNormalCommand(cmdLine)
//}


// Remove the given key from db
func (db *DB) Remove(key string) {
	db.stopWorld.Wait()
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}


/* ---- TTL Functions ---- */
func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWorld.Wait()
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)

		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}

		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})

}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.stopWorld.Wait()
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}