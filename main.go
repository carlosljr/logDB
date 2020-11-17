package main

import (
	"fmt"
	"os"
	"strings"
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

	fmt.Println("Ready for action!")
}
