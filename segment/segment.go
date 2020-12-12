package segment

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

var (
	filePath = "./log_storage"
)

type Segment struct {
	hashTable  map[string]int
	LineNumber int
	LogFile    string
}

func (s *Segment) writeLineIntoFile(key, value string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		_ = os.Mkdir(filePath, os.ModePerm)
	}

	fileNamePath := fmt.Sprintf("%s/%s", filePath, s.LogFile)

	file, err := os.OpenFile(fileNamePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	lineData := fmt.Sprintf("%s,%s\n", key, value)

	if _, err := file.WriteString(lineData); err != nil {
		return err
	}

	return nil
}

// SetKeyValueIntoSegment append a {key,value} line into
// file.
func (s *Segment) SetKeyValueIntoSegment(key, value string) error {
	// Write key and value into file
	if err := s.writeLineIntoFile(key, value); err != nil {
		return err
	}

	// Insert key and index on hash table
	if s.hashTable == nil {
		s.hashTable = make(map[string]int)
	}
	s.hashTable[key] = s.LineNumber
	s.LineNumber += 1

	return nil
}

func (s *Segment) getIndexKey(key string) (int, error) {
	if v, ok := s.hashTable[key]; !ok {
		return 0, errors.New("Key not found in the hash table")
	} else {
		return v, nil
	}
}

func (s *Segment) getFileLines() ([]string, error) {
	logFilePath := fmt.Sprintf("%s/%s", filePath, s.LogFile)
	file, err := os.Open(logFilePath)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Split(bufio.ScanLines)
	var lines []string

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines, nil
}

// GetValueFromSegment searches for the key in segment file
// from the key index in hash table. If key is not found,
// it will return an error.
func (s *Segment) GetValueFromSegment(key string) (string, error) {
	// Get the index in hash table
	index, err := s.getIndexKey(key)

	if err != nil {
		return "", err
	}

	// Returns the file
	// Transforms lines in array
	fileLines, err := s.getFileLines()

	if err != nil {
		return "", err
	}

	// Get the line correspondent to the index
	lineData := fileLines[index]

	// Get value
	value := strings.SplitN(lineData, ",", 2)[1]

	return value, nil
}

// DeleteMe removes the segment file
func (s *Segment) DeleteMe() error {
	logFilePath := fmt.Sprintf("%s/%s", filePath, s.LogFile)

	if err := os.Remove(logFilePath); err != nil {
		return err
	}

	return nil
}

func (s *Segment) resetMe(keyValues map[string]string) error {
	if err := s.DeleteMe(); err != nil {
		return err
	}

	s.LineNumber = 0
	for key, value := range keyValues {
		if err := s.SetKeyValueIntoSegment(key, value); err != nil {
			return err
		}
	}

	return nil
}

// Compact the segment file to keep the newer key/value
// pairs, removing redundant keys with old values.
// The newer key/value pairs and error in
// compact operation are returned.
func (s *Segment) Compact() (map[string]string, error) {
	fileLines, err := s.getFileLines()

	if err != nil {
		return nil, err
	}

	recentKeysValues := make(map[string]string)
	for i, line := range fileLines {
		keyValue := strings.SplitN(line, ",", 2)
		key := keyValue[0]
		value := keyValue[1]

		// It will get the current index from the key
		// and verify if the index corresponds to the
		// current line.
		if currentIndex, ok := s.hashTable[key]; !ok {
			errorMsg := fmt.Sprintf("Could not find index for key %s", key)
			return nil, errors.New(errorMsg)
		} else if currentIndex == i {
			recentKeysValues[key] = value
		}
	}

	return recentKeysValues, s.resetMe(recentKeysValues)
}

// LoadExistingData loads all data from existing segment file
// and mount its hash table.
func (s *Segment) LoadExistingData() error {
	fileLines, err := s.getFileLines()

	if err != nil {
		return err
	}

	if s.hashTable == nil {
		s.hashTable = make(map[string]int)
	}

	for i, fileLine := range fileLines {
		key := strings.Split(fileLine, ",")[0]
		s.hashTable[key] = i
	}
	s.LineNumber = len(fileLines)

	return nil
}
