package command

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/carlosljr/logDB/segment"
)

type Command struct {
	CurrentSegment          *segment.Segment
	Segments                []*segment.Segment
	SegmentSize             int
	CompactAndMergeInterval int
	numberOfSegments        int
}

// CompactAndMerge makes compaction
// for existing segments and merge them
// in a estipulated interval.
func (c *Command) CompactAndMerge() {
	for {
		time.Sleep(time.Duration(c.CompactAndMergeInterval) * time.Second)
		if len(c.Segments) < 2 {
			fmt.Println("Not enough segments to compact and merge")
			continue
		}
		segmentsLessCurrent := c.Segments[:len(c.Segments)-1]

		mergedKeyValues := make(map[string]string)
		compactWithSuccess := true
		for _, s := range segmentsLessCurrent {
			recentKeyValues, err := s.Compact()
			if err != nil {
				fmt.Fprintf(os.Stderr, "\n\nCould not compact %s segment: %v\n\n", s.LogFile, err)
				compactWithSuccess = false
				break
			}
			for key, value := range recentKeyValues {
				mergedKeyValues[key] = value
			}
		}

		if !compactWithSuccess {
			continue
		}

		if len(segmentsLessCurrent) == 1 {
			continue
		}

		mergedSegment := &segment.Segment{
			LogFile: fmt.Sprintf("logfile-merged_%d.log", c.numberOfSegments+1),
		}

		mergedWithSuccess := true
		for key, value := range mergedKeyValues {
			if err := mergedSegment.SetKeyValueIntoSegment(key, value); err != nil {
				fmt.Fprintf(os.Stderr, "\n\nCould not generate merged segment %s: %v\n\n", mergedSegment.LogFile, err)
				mergedWithSuccess = false
				break
			}
		}

		if !mergedWithSuccess {
			// Auto delete merged segment
			mergedSegment.DeleteMe()
			continue
		}

		// Remove segments from slice
		c.Segments = c.Segments[len(c.Segments)-1:]

		// Keep only merged and current segment
		c.Segments[0] = mergedSegment
		c.Segments = append(c.Segments, c.CurrentSegment)

		for _, s := range segmentsLessCurrent {
			// Auto delete segment that was merged
			s.DeleteMe()
		}

		c.numberOfSegments += 1

	}
}

func getNumberFromSegment(fileName string) int {
	logFilePrefix := strings.Split(fileName, ".")[0]
	fileNumberStr := strings.Split(logFilePrefix, "_")[1]
	fileNumber, _ := strconv.Atoi(fileNumberStr)

	return fileNumber
}

func (c *Command) sortAndUpdateSegments(segments []*segment.Segment) {

	var logFileNumbers []int

	for _, s := range segments {
		logFileNumbers = append(logFileNumbers, getNumberFromSegment(s.LogFile))
	}
	sort.Ints(logFileNumbers)

	lastSegmentNumber := logFileNumbers[len(logFileNumbers)-1]

	for _, segmentNumber := range logFileNumbers {
		for _, s := range segments {
			if strings.Contains(s.LogFile, strconv.Itoa(segmentNumber)) {
				c.Segments = append(c.Segments, s)
				break
			}
		}
	}

	if lastSegmentNumber > c.numberOfSegments {
		c.numberOfSegments = lastSegmentNumber
	}

}

// LoadExistingSegments will be called in logDB boot.
// It will load existing segments and build their hash table
// map to index existing keys.
func (c *Command) LoadExistingSegments(logFiles []string) error {
	var mergedSegments []*segment.Segment
	var rawSegments []*segment.Segment
	for _, logFile := range logFiles {
		s := &segment.Segment{
			LogFile: logFile,
		}
		// Load data from segment file and build hash table
		if err := s.LoadExistingData(); err != nil {
			fmt.Fprintf(os.Stderr, "\n\nFailed during logFile %s load process: %v\n\n", s.LogFile, err)
			return err
		}
		if strings.Contains(s.LogFile, "merged") {
			mergedSegments = append(mergedSegments, s)
		} else {
			rawSegments = append(rawSegments, s)
		}

	}
	c.sortAndUpdateSegments(mergedSegments)
	c.sortAndUpdateSegments(rawSegments)

	c.CurrentSegment = c.Segments[len(c.Segments)-1]

	return nil
}

// SetValueIntoLog stores a key and value in current
// segment.
func (c *Command) SetValueIntoLog(key, value string) error {
	if c.CurrentSegment == nil || c.CurrentSegment.LineNumber >= c.SegmentSize {
		c.CurrentSegment = &segment.Segment{
			LogFile: fmt.Sprintf("logfile_%d.log", c.numberOfSegments+1),
		}
		c.Segments = append(c.Segments, c.CurrentSegment)
		c.numberOfSegments += 1
	}

	// Insert key and value
	return c.CurrentSegment.SetKeyValueIntoSegment(key, value)
}

// GetValueFromKey retrieves value from key passed as argument.
// It will search from the current segment to old ones following
// theit existence order. If key is not found, it will return an error.
func (c *Command) GetValueFromKey(key string) (string, error) {
	var value string
	var err error

	if len(c.Segments) == 0 {
		return value, errors.New("Your database is empty.")
	}

	for i := len(c.Segments) - 1; i >= 0; i-- {
		s := c.Segments[i]

		value, err = s.GetValueFromSegment(key)

		if err != nil {
			if err.Error() == "Key not found in the hash table" {
				continue
			}
			break
		}
		break
	}
	return value, err
}
