# LevelDB MemDB Documentation

package memdb // import "github.com/syndtr/goleveldb/leveldb/memdb"

Package memdb provides in-memory key/value database implementation.

var ErrNotFound = errors.ErrNotFound ...
type DB struct{ ... }
    func New(cmp comparer.BasicComparer, capacity int) *DB
