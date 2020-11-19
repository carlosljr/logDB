package command

import (
	"fmt"

	"github.com/carlosljr/logDB/segment"
)

type Command struct {
	CurrentSegment *segment.Segment
	Segments       []*segment.Segment
}

func (c *Command) SetValueIntoLog(key, value string) error {
	if len(c.Segments) == 0 {
		c.Segments = append(c.Segments, c.CurrentSegment)
	}
	if c.CurrentSegment.LineNumber == 3 {
		c.CurrentSegment = &segment.Segment{
			LogFile: fmt.Sprintf("logfile_%d.log", len(c.Segments)+1),
		}
		c.Segments = append(c.Segments, c.CurrentSegment)
	}

	// Inserir chave e valor no arquivo de log. Retorna area de memoria inserida
	return c.CurrentSegment.SetKeyValueIntoSegment(key, value)
}
