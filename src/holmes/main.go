package main

var holmesConf HolmesConfig

func main() {
	confFile := "holmes.conf"
	ua_pattern_file := "../data/user_agent_pattern.json"
	holmesConf = LoadConfig(confFile)
	InitUAParsers(ua_pattern_file)
	Filter(holmesConf)
}
