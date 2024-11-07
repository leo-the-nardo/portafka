package env

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Getenv Busca variaveis de ambiente na seguinte ordem de prioridade : .env file > OS Envs > Default envs
func Getenv(env string) string {
	if val := viper.GetString(env); val != "" {
		return val
	}
	osEnv := os.Getenv(env)
	if osEnv == "" {
		log.Default().Fatalf("Environment variable %s not set and no default available", env)
	}
	return osEnv
}

// Load Carrega os arquivos de envs e seta os valores default caso não existam no arquivo e por fim loga os envs (oculta os prefixos 'SENSITIVE_')
func Load(defaults map[string]string, envFiles ...string) {
	for k, v := range defaults {
		viper.SetDefault(k, v)
	}
	for _, env := range envFiles {
		viper.SetConfigFile(env)
		if err := viper.ReadInConfig(); err != nil {
			log.Default().Printf("ENV Info: Unable to load %s file. Application will try to use OS Envs instead.", env)
		}
	}
	logEnvs()
}

func logEnvs() {
	defaults := viper.AllSettings()
	concatenateDefaultsToString := ""
	for k, v := range defaults {
		k = strings.ToUpper(k)
		prefixToHide := "SENSITIVE_"
		if strings.HasPrefix(k, prefixToHide) {
			concatenateDefaultsToString += fmt.Sprintf("%s: %s | ", k, "**********")
			continue
		}
		concatenateDefaultsToString += fmt.Sprintf("%s: %s | ", k, v)
	}
	log.Default().Println(concatenateDefaultsToString)
}
