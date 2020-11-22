package command

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/carlosljr/logDB/segment"
)

type Command struct {
	CurrentSegment   *segment.Segment
	Segments         []*segment.Segment
	numberOfSegments int
}

func (c *Command) CompactAndMerge() {
	for {
		time.Sleep(30 * time.Second)
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
			// Ter uma funcao no segmento que se auto deleta
			mergedSegment.DeleteMe()
			continue
		}

		// Remover os segmentos mergeados e remover do slice

		// Remover segmentos do slice
		c.Segments = c.Segments[len(c.Segments)-1:]

		// Inserir na primeira posição
		c.Segments[0] = mergedSegment
		c.Segments = append(c.Segments, c.CurrentSegment)

		for _, s := range segmentsLessCurrent {
			// Chamar funcao do segmento que se auto deleta
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

func (c *Command) LoadExistingSegments(logFiles []string) error {
	var mergedSegments []*segment.Segment
	var rawSegments []*segment.Segment
	for _, logFile := range logFiles {
		s := &segment.Segment{
			LogFile: logFile,
		}
		// Carrega os dados do log e gera a hash table
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

func (c *Command) SetValueIntoLog(key, value string) error {
	if c.CurrentSegment == nil || c.CurrentSegment.LineNumber >= 3 {
		c.CurrentSegment = &segment.Segment{
			LogFile: fmt.Sprintf("logfile_%d.log", c.numberOfSegments+1),
		}
		c.Segments = append(c.Segments, c.CurrentSegment)
		c.numberOfSegments += 1
	}

	// Inserir chave e valor no arquivo de log. Retorna area de memoria inserida
	return c.CurrentSegment.SetKeyValueIntoSegment(key, value)
}

func (c *Command) GetValueFromKey(key string) (string, error) {
	var value string
	var err error
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
