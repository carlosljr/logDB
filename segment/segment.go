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

func (s *Segment) SetKeyValueIntoSegment(key, value string) error {
	// Escreve chave e valor no arquivo
	if err := s.writeLineIntoFile(key, value); err != nil {
		return err
	}

	//Insere chave e index na hash_table
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

func (s *Segment) GetValueFromSegment(key string) (string, error) {
	index, err := s.getIndexKey(key)

	if err != nil {
		return "", err
	}

	// Retorna o arquivo
	// Transforma o conjunto de linhas em um array
	fileLines, err := s.getFileLines()

	if err != nil {
		return "", err
	}

	// Pega a linha correspondente ao index
	lineData := fileLines[index]

	// Pega o valor correspondente
	value := strings.Split(lineData, ",")[1]

	return value, nil
}

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

func (s *Segment) Compact() (map[string]string, error) {
	fileLines, err := s.getFileLines()

	if err != nil {
		return nil, err
	}

	recentKeysValues := make(map[string]string)
	for i, line := range fileLines {
		keyValue := strings.Split(line, ",")
		key := keyValue[0]
		value := keyValue[1]

		if currentIndex, ok := s.hashTable[key]; !ok {
			errorMsg := fmt.Sprintf("Could not find index for key %s", key)
			return nil, errors.New(errorMsg)
		} else if currentIndex == i {
			recentKeysValues[key] = value
		}
	}

	return recentKeysValues, s.resetMe(recentKeysValues)
}

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
