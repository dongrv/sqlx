package example

import "testing"

func TestDo(t *testing.T) {
	if err := Do(); err != nil {
		t.Fatal(err)
	}
}
