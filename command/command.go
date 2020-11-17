package command

import (
	"fmt"

	"github.com/carlosljr/logDB/segment"
)

type Command struct {
	CurrentSegment *segment.Segment
	Segments       []*segment.Segment
}

func (c Command) SetValueIntoLog(key, value string) error {
	if c.CurrentSegment == nil || c.CurrentSegment.LineNumber == 25 {
		c.CurrentSegment = &segment.Segment{
			LogFile: fmt.Sprintf("logfile_%d.log", len(c.Segments)+1),
		}
		c.Segments = append(c.Segments, c.CurrentSegment)
	}

	// Inserir chave e valor no arquivo de log. Retorna area de memoria inserida
	return c.CurrentSegment.SetKeyValueIntoSegment(key, value)
}
