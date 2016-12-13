package skeleton

import (
	"fmt"
	"strings"
	"testing"

	"github.com/fortytw2/leaktest"
)

func TestConfig(t *testing.T) {
	defer leaktest.Check(t)()

	testCases := []struct {
		c   Config
		err string
	}{
		{
			c:   DefaultConfig,
			err: "",
		},
		{
			c: Config{
				MaxKeysPerNode: 1,
				MaxDeltaCount:  1,
				GCTime:         0,
			},
			err: "",
		},
		{
			c: Config{
				MaxKeysPerNode: 0,
				MaxDeltaCount:  1,
				GCTime:         0,
			},
			err: "MaxKeysPerNode",
		},
		{
			c: Config{
				MaxKeysPerNode: 1,
				MaxDeltaCount:  0,
				GCTime:         0,
			},
			err: "MaxDeltaCount",
		},
		{
			c: Config{
				MaxKeysPerNode: 1,
				MaxDeltaCount:  1,
				GCTime:         -1,
			},
			err: "GCTime",
		},
	}
	for i, tc := range testCases {
		if err := tc.c.Verify(); !strings.Contains(fmt.Sprintf("%s", err), tc.err) {
			t.Errorf("%d: %v.Verify() = %+v; expected %q", i, tc.c, err, tc.err)
		}
	}
}
