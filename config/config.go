package config


// ServerProperties defines global config properties
type ServerProperties struct {
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	AppendOnly     bool   `cfg:"appendOnly"`
	AppendFilename string `cfg:"appendFilename"`
	MaxClients     int    `cfg:"maxclients"`
	RequirePass    string `cfg:"requirepass"`
	Databases      int    `cfg:"databases"`
	RDBFilename    string `cfg:"dbfilename"`

	Peers []string `cfg:"peers"`
	Self  string   `cfg:"self"`
}

// Properties holds global config properties
var Properties *ServerProperties