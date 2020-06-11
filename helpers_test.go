package producer

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func loadJSONFromFile(t *testing.T, name string, out interface{}) {
	require.NotNil(t, out)
	b := loadBytesFromFile(t, name)
	err := json.Unmarshal(b, out)
	require.NoError(t, err)
}

func loadBytesFromFile(t *testing.T, name string) []byte {
	file, err := os.Open(name)
	require.NoError(t, err)
	defer file.Close()
	b, err := ioutil.ReadAll(file)
	require.NoError(t, err)
	return b
}
