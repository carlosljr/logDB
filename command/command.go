package command

import (
	"fmt"
	"os"
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
			fmt.Printf("\n\nRecent key values for %s: %v\n\n", s.LogFile, recentKeyValues)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\n\nCould not compact %s segment: %v\n\n", s.LogFile, err)
				compactWithSuccess = false
				break
			}
			for key, value := range recentKeyValues {
				mergedKeyValues[key] = value
			}
		}

		fmt.Printf("\n\nMerged key values: %v\n\n", mergedKeyValues)

		if !compactWithSuccess {
			continue
		}

		if len(segmentsLessCurrent) == 1 {
			continue
		}

		mergedSegment := &segment.Segment{
			LogFile: fmt.Sprintf("logfile_%d.log", c.numberOfSegments+1),
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

		// Definir novo segmento no slice

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
