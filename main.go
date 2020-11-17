package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/carlosljr/logDB/command"
	"github.com/carlosljr/logDB/command/get"
)

func throwInputError(action string) {
	if action != "" {
		fmt.Fprintf(os.Stderr, `Invalid command %s. Please, insert one of these commands:
		get {key} - to retrieve a value from it correspondent {key}
		set {key, value} - to set a new or update key/value pair
	`, action)
		os.Exit(1)
	}

	fmt.Fprint(os.Stderr, `No arguments inserted. Please insert one of these commands:
		get {key} - to retrieve a value from it correspondent {key}
		set {key, value} - to set a new or update key/value pair
		`)
	os.Exit(1)
}

func hasValidGetArguments(arguments []string) bool {
	if len(arguments) != 1 || arguments[0] == "" {
		return false
	}
	return true
}

func hasValidSetArguments(arguments []string) bool {
	if len(arguments) != 2 || arguments[0] == "" {
		return false
	}

	return true
}

func hasValidArguments(action string, arguments []string) bool {
	if strings.EqualFold(action, "get") {
		return hasValidGetArguments(arguments)
	}
	return hasValidSetArguments(arguments)
}

func main() {
	if len(os.Args) == 1 {
		throwInputError("")
	}
	my_args := os.Args[1:]
	valid_cmds := []string{"get", "set"}
	action := my_args[0]
	cmd_args := my_args[1:]
	is_valid_action := false

	for _, cmd := range valid_cmds {
		if strings.EqualFold(action, cmd) && len(cmd_args) != 0 && hasValidArguments(action, cmd_args) {
			is_valid_action = true
			break
		}
	}

	if !is_valid_action {
		throwInputError(action)
	}

	var err error
	var value string
	key := cmd_args[0]

	command := command.Command{}

	if strings.EqualFold(action, "get") {
		// Chama função que retorna o valor associado a chave
		if value, err = get.GetValueFromKey(key); err != nil {
			fmt.Fprintf(os.Stderr, "Error during get command: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s\n", value)
	}

	if strings.EqualFold(action, "set") {
		value = cmd_args[1]
		// caso seja uma escrita, chama função que escreve. Verifica retorno para ver se houve erro
		if err = command.SetValueIntoLog(key, value); err != nil {
			fmt.Fprintf(os.Stderr, "Error during set command: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Value %s stored with success\n", value)
	}
}
