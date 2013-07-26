package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

type RedisConf struct {
	Network        string
	Address        string
	ConnectTimeout int64
	ReadTimeout    int64
	WriteTimeout   int64
	BlockTimeout   int64
}

type RedisConn struct {
	conn redis.Conn
}

type Slowlog struct {
	Id            int64
	Log_timestamp int64
	Time_consumed int64
	Cmd           string
}

func NewRedisConn(redisConf RedisConf) *RedisConn {
	c, err := redis.DialTimeout(redisConf.Network, redisConf.Address, time.Duration(redisConf.ConnectTimeout), time.Duration(redisConf.ReadTimeout), time.Duration(redisConf.WriteTimeout))
	if err != nil {
		log.Fatal("(NewRedisConn) ", err)
	}
	return &RedisConn{
		conn: c,
	}
}

func (redisConn *RedisConn) Close() {
	redisConn.conn.Close()
}

///////////////////////////////////////////////////////////////////////////////
// Keys operation
///////////////////////////////////////////////////////////////////////////////

// GetKeys return the keys match the pattern in redis
// output:a keys string slice
func (redisConn *RedisConn) GetKeys(pattern string) []string {
	keys := make([]string, 0, 16)
	if redisConn != nil {
		r, err := redisConn.conn.Do("KEYS", pattern)
		if err != nil {
			log.Panic("(GetKeys) ", err)
		}
		if r != nil {
			v, err := redis.Values(r, err)
			if err != nil {
				log.Panic("(GetKeys) ", err)
			}
			for _, key := range v {
				keys = append(keys, string(key.([]uint8)))
			}
		}
	}
	return keys
}

// KeyType return the string representation of key
// output:none,string,list,hash,set,zset
func (redisConn *RedisConn) KeyType(key string) string {
	var keyType string
	if redisConn != nil {
		r, err := redisConn.conn.Do("TYPE", key)
		if err != nil {
			log.Panic("(KeyType) ", err)
		}
		keyType = string(r.([]uint8))
	}
	return keyType
}

func (redisConn *RedisConn) KeyDel(key string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("DEL", key)
		if err != nil {
			panic(err)
		}
		result = r.(int64)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////
// Strings operation
///////////////////////////////////////////////////////////////////////////////

// Set set a key value pair in redis
// output:return string "OK"
func (redisConn *RedisConn) Set(key string, value string) string {
	var result string
	if redisConn != nil {
		r, err := redisConn.conn.Do("SET", key, value)
		if err != nil {
			log.Panic("(Set) ", err)
		}
		result = r.(string)
	}
	return result
}

// Get return a value of a key
// output:1)if the key exist and is a string, return its value,
//        2)else,return null string
func (redisConn *RedisConn) Get(key string) string {
	var result string
	if redisConn != nil {
		r, err := redisConn.conn.Do("GET", key)
		if err != nil {
			log.Panic("(Get) ", err)
		}
		result = r.(string)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////
// Hashs operation
///////////////////////////////////////////////////////////////////////////////

// HashSet set a field to value if the field is not exist,or update the value of
// the field
// input:
//     1)key which represent the hash table;
//     2)field;
//     3)value
// output:
//     if the field is not exist,return 1,else return 0
func (redisConn *RedisConn) HashSet(ht string, field string, value string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("HSET", ht, field, value)
		if err != nil {
			log.Panic("(HashSet) ", err)
		}
		result = r.(int64)
	}
	return result
}

func (redisConn *RedisConn) HashGet(ht string, field string) string {
	var result string
	if redisConn != nil {
		r, err := redisConn.conn.Do("HGET", ht, field)
		if err != nil {
			log.Panic("(HashGet) ", err)
		}
		result = string(r.([]uint8))
	}
	return result
}

func (redisConn *RedisConn) HashIncrby(ht string, field string, increment int) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("HINCRBY", ht, field, increment)
		if err != nil {
			log.Panic("(HashIncrby) ", err)
		}
		result = r.(int64)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////
// Lists operation
///////////////////////////////////////////////////////////////////////////////

// ListLen return the lenght of a list
// output:the lenght of list
func (redisConn *RedisConn) ListLen(list string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("LLEN", list)
		if err != nil {
			log.Panic("(ListLen) ", err)
		}
		result = r.(int64)
	}
	return result
}

func (redisConn *RedisConn) ListRange(list string, start, end int) []string {
	items := make([]string, 0, 16)
	if redisConn != nil {
		r, err := redisConn.conn.Do("LRANGE", list, start, end)
		if err != nil {
			log.Panic("(ListRange) ", err)
		}
		if r != nil {
			v, err := redis.Values(r, err)
			if err != nil {
				log.Panic("(ListRange) ", err)
			}
			for _, item := range v {
				items = append(items, string(item.([]uint8)))
			}
		}
	}
	return items
}

// ListLeftPush push an item into a list at the left side of the list
// output:the lenght of list after push this item
func (redisConn *RedisConn) ListLeftPush(list, item string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("LPUSH", list, item)
		if err != nil {
			log.Panic("(ListLeftPush) ", err)
		}
		result = r.(int64)
	}
	return result
}

// ListLeftPop return the most left side element of a list
// output:if list a items return the most left side element,else,return null string
func (redisConn *RedisConn) ListLeftPop(list string) string {
	var result string
	if redisConn != nil {
		r, err := redisConn.conn.Do("LPOP", list)
		if err != nil {
			log.Panic("(ListLeftPop) ", err)
		}
		if r == nil {
			result = ""
		} else {
			result = string(r.([]uint8))
		}
	}
	return result
}

// ListRightPush push an item into a list at the right side of the list
// output:the lenght of list after push this item
func (redisConn *RedisConn) ListRightPush(list, item string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("RPUSH", list, item)
		if err != nil {
			log.Panic("(ListRightPush) ", err)
		}
		result = r.(int64)
	}
	return result
}

// ListRightPop return the most right side element of a list
// output:if list a items return the most right side element,else,return null string
func (redisConn *RedisConn) ListRightPop(list string) string {
	var result string
	if redisConn != nil {
		r, err := redisConn.conn.Do("RPOP", list)
		if err != nil {
			log.Panic("(ListRightPop) ", err)
		}
		if r == nil {
			result = ""
		} else {
			result = string(r.([]uint8))
		}
	}
	return result
}

// BlockListLeftPop return the most left side element of a list,when the list we want to
// pop have no element,block at most timeout seconds
// input:
//     1)list name type of string;
//     2)timeout second type of int64
// output:
//     if success,return a <list,item> pair;else return a <"",""> pair
func (redisConn *RedisConn) BlockListLeftPop(list string, timeout int64) (string, string) {
	if redisConn != nil {
		r, err := redisConn.conn.Do("BLPOP", list, timeout)
		if err != nil {
			log.Panic("(BlockListLeftPop) ", err)
		}
		if r != nil {
			v, err := redis.Values(r, err)
			if err != nil {
				log.Panic("(BlockListLeftPop) ", err)
			}
			listname := string(v[0].([]uint8))
			item := string(v[1].([]uint8))
			return listname, item
		}
	}
	return "", ""
}

// BlockListRightPop return the most right side element of a list,when the list we want to
// pop have no element,block at most timeout seconds
// input:
//     1)list name type of string;
//     2)timeout second type of int64
// output:
//     if success,return a <list,item> pair;else return a <"",""> pair
func (redisConn *RedisConn) BlockListRightPop(list string, timeout int64) (string, string) {
	if redisConn != nil {
		r, err := redisConn.conn.Do("BRPOP", list, timeout)
		if err != nil {
			log.Panic("(BlockListRightPop) ", err)
		}
		if r != nil {
			v, err := redis.Values(r, err)
			if err != nil {
				log.Panic("(BlockListRightPop) ", err)
			}
			listname := string(v[0].([]uint8))
			item := string(v[1].([]uint8))
			return listname, item
		}
	}
	return "", ""
}

///////////////////////////////////////////////////////////////////////////////
// Sets operation
///////////////////////////////////////////////////////////////////////////////

func (redisConn *RedisConn) SetAdd(set string, member string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("SADD", set, member)
		if err != nil {
			log.Panic("(SetAdd) ", err)
		}
		result = r.(int64)
	}
	return result
}

func (redisConn *RedisConn) SetRem(set string, member string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("SREM", set, member)
		if err != nil {
			log.Panic("(SetRem) ", err)
		}
		result = r.(int64)
	}
	return result
}

func (redisConn *RedisConn) SetIsMember(set string, member string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("SISMEMBER", set, member)
		if err != nil {
			log.Panic("(SetIsMember) ", err)
		}
		result = r.(int64)
	}
	return result
}

// SetCard returns the set cardinality (number of elements) of the set stored at set
func (redisConn *RedisConn) SetCard(set string) int64 {
	var result int64
	if redisConn != nil {
		r, err := redisConn.conn.Do("SCARD", set)
		if err != nil {
			log.Panic("(SetCard) ", err)
		}
		result = r.(int64)
	}
	return result
}

// SetMembers returns all the members of the set value stored at set
func (redisConn *RedisConn) SetMembers(set string) []string {
	members := make([]string, 0, 16)
	if redisConn != nil {
		r, err := redisConn.conn.Do("SMEMBERS", set)
		if err != nil {
			log.Panic("(SetMembers) ", err)
		}
		if r != nil {
			v, err := redis.Values(r, err)
			if err != nil {
				log.Panic("(SetMembers) ", err)
			}
			for _, member := range v {
				members = append(members, string(member.([]uint8)))
			}
		}
	}
	return members
}

///////////////////////////////////////////////////////////////////////////////
// Sorted Sets operation
///////////////////////////////////////////////////////////////////////////////

// TODO

///////////////////////////////////////////////////////////////////////////////
// Pub/Sub operation
///////////////////////////////////////////////////////////////////////////////

// TODO

///////////////////////////////////////////////////////////////////////////////
// Transactions operation
///////////////////////////////////////////////////////////////////////////////

// TODO

///////////////////////////////////////////////////////////////////////////////
// Scripting operation
///////////////////////////////////////////////////////////////////////////////

// TODO

///////////////////////////////////////////////////////////////////////////////
// Connection operation
///////////////////////////////////////////////////////////////////////////////

// TODO

///////////////////////////////////////////////////////////////////////////////
// Server operation
///////////////////////////////////////////////////////////////////////////////

// TODO
func (redisConn *RedisConn) GetSlowlog() []Slowlog {
	slowlogs := make([]Slowlog, 0, 16)
	if redisConn != nil {
		r, err := redisConn.conn.Do("slowlog", "get")
		if err != nil {
			log.Panic("(GetSlowlog) ", err)
		}

		slogs, errForValues := redis.Values(r, err) // convert interface{} to []interface{}
		if errForValues != nil {
			log.Panic("(GetSlowlog) ", errForValues)
		}
		for _, slog := range slogs { // each log is type of interface{}
			var slowlog Slowlog
			slog_items, errForValues := redis.Values(slog, err) // convert interface{} to []interface{}

			if errForValues != nil {
				log.Panic("(GetSlowlog) ", errForValues)
			}
			for i, slog_item := range slog_items { // each log item is type of interface{}
				switch slog_item.(type) {
				case int64:
					if i == 0 {
						slowlog.Id = slog_item.(int64)
					} else if i == 1 {
						slowlog.Log_timestamp = slog_item.(int64)
					} else if i == 2 {
						slowlog.Time_consumed = slog_item.(int64)
					}

				case interface{}: // each cmd is type of interface{}
					cmd_items, errForValues := redis.Values(slog_item, err) //  get each cmd item
					if errForValues != nil {
						log.Panic("(GetSlowlog) ", errForValues)
					}
					var cmd string
					for _, cmd_item := range cmd_items {
						cmd = cmd + string(cmd_item.([]uint8)) + " "
					}
					slowlog.Cmd = cmd
				}
			} // end of loop for each log
			slowlogs = append(slowlogs, slowlog)
		} // end of loop for all logs
	} // end of main if
	return slowlogs
}
