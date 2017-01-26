package agent

import "github.com/jonbonazza/huton/lib"

type Config struct {
	huton.Config `json:",inline" yaml:",inline"`
}

func DefaultConfig() *Config {
	return &Config{
		Config: huton.Config{
			BindAddr: "0.0.0.0",
			BindPort: 8080,
		},
	}
}
