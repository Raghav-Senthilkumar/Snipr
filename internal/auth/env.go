package auth

import (
	"bufio"
	"os"
	"strings"
)

// LoadEnv loads key=value pairs from a .env file into the process environment.
// Existing environment variables take precedence and will not be overwritten.
func LoadEnv(path string) error {
	if path == "" {
		path = ".env"
	}

	f, err := os.Open(path)
	if err != nil {
		return err // .env file is optional
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			// Strip surrounding quotes if present
			val = strings.Trim(val, `"'`)

			if os.Getenv(key) == "" {
				_ = os.Setenv(key, val)
			}
		}
	}

	return scanner.Err()
}
