package sqlx

import "errors"

type ConfigMap map[string]Config

func (cmp ConfigMap) Set(connName string, config Config) error {
	if _, ok := cmp[connName]; ok {
		return errors.New("exists connection config")
	}
	cmp[connName] = config
	return nil
}

func (cmp ConfigMap) Validate() bool {
	for _, c := range cmp {
		if !c.Validate() {
			return false
		}
	}
	return true
}
