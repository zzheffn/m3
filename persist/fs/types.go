	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3x/instrument"
// FileSetSeeker provides an out of order reader for a TSDB file set
type FileSetSeeker interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(namespace ts.ID, shard uint32, start time.Time) error

	// Seek returns the data for specified id provided the index was loaded upon open. An
	// error will be returned if the index was not loaded or id cannot be found
	Seek(id ts.ID) (data []byte, err error)

	// Range returns the time range associated with data in the volume
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int
}

	// SetRateLimitOptions sets the rate limit options
	SetRateLimitOptions(value ratelimit.Options) Options

	// RateLimitOptions returns the rate limit options
	RateLimitOptions() ratelimit.Options
