package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/carlosljr/logDB/command"
)

func throwInputError(action string) {
	if action != "" {
		fmt.Fprintf(os.Stderr, "\n\nERROR:\nInvalid command %s. You need to insert a key.\ne.g.\nget {key} - to retrieve a value from it correspondent {key}\nset {key} - to set a new or update key/value pair\n\n", action)
		return
	}

	fmt.Fprint(os.Stderr, "\n\nERROR:\nNo arguments inserted. Please, insert one of these commands:\nget {key} - to retrieve a value from it correspondent {key}\nset {key} - to set a new or update key/value pair\n\n")
}

func currentLogFiles() []string {
	filePath := "./log_storage"
	var logFiles []string

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return logFiles
	}

	err := filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		logFiles = append(logFiles, info.Name())
		return nil
	})

	if err != nil {
		return nil
	}

	return logFiles
}

func main() {

	fmt.Printf("\nWelcome to LogDB. We support the following commands bellow:\n\n")
	fmt.Printf("get {key} - to retrieve a value from it correspondent {key}\n")
	fmt.Printf("set {key} - to set a new or update key/value pair\n")
	fmt.Printf("exit - Leave LogDB\n\n")

	command := &command.Command{}

	logFiles := currentLogFiles()

	if len(logFiles) != 0 {
		// Carrega os arquivos de log e existentes
		// e cria os segmentos com suas respectivas hash tables
		// fmt.Println("Logfiles:", logFiles)
		if err := command.LoadExistingSegments(logFiles); err != nil {
			return
		}
	}

	go command.CompactAndMerge()

	for {
		fmt.Printf("Insert your command and press enter:\n\n")
		fmt.Print("-> ")

		var action string
		var key string
		var value string

		fmt.Scanf("%s %s", &action, &key)

		valid_cmds := []string{"get", "set"}

		if action == "" {
			throwInputError("")
			continue
		}

		if strings.EqualFold(action, "exit") {
			break
		}

		is_valid_action := false
		for _, cmd := range valid_cmds {
			if strings.EqualFold(action, cmd) && key != "" {
				is_valid_action = true
				break
			}
		}

		if !is_valid_action {
			throwInputError(action)
			continue
		}

		var err error

		if strings.EqualFold(action, "get") {
			// Chama função que retorna o valor associado a chave
			if value, err = command.GetValueFromKey(key); err != nil {
				fmt.Fprintf(os.Stderr, "\n\nError during get command: %v\n\n", err)
				continue
			}
			fmt.Printf("\nResult:\n\n-> %s\n\n", value)
		}

		if strings.EqualFold(action, "set") {
			reader := bufio.NewReader(os.Stdin)
			fmt.Printf("\n\nInsert the value for this key and press enter:\n\n")
			fmt.Print("-> ")
			value, _ = reader.ReadString('\n')
			value = strings.Replace(value, "\n", "", -1)
			// caso seja uma escrita, chama função que escreve. Verifica retorno para ver se houve erro
			if err = command.SetValueIntoLog(key, value); err != nil {
				fmt.Fprintf(os.Stderr, "\n\nError during set command: %v\n\n", err)
				continue
			}
			fmt.Printf("\nValue %s stored with success\n\n", value)
		}
	}
	// filePath := "./log_storage"

	// if _, err := os.Stat(filePath); !os.IsNotExist(err) {
	// 	err := os.RemoveAll("./log_storage")
	// 	if err != nil {
	// 		fmt.Fprintf(os.Stderr, "\n\nCould not remove log file storage: %v\n\n", err)
	// 	}
	// }

	fmt.Fprintf(os.Stdout, "\nSee ya!\n\n")
	os.Exit(0)
}
