package get

func GetValueFromKey(key string) (string, error) {
	return key + ": my_val", nil
}
