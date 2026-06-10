package goutils

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config represents the absolute root configuration block for your enterprise architecture.
type Config struct {
	App       AppConfig                 `yaml:"app"`
	Logging   LoggingConfig             `yaml:"logging"`
	Servers   map[string]ServerConfig   `yaml:"servers,omitempty"`
	Services  map[string]ServicesConfig `yaml:"services,omitempty"`
	Workers   map[string]WorkerConfig   `yaml:"workers,omitempty"`
	Databases map[string]DatabaseConfig `yaml:"databases,omitempty"`
	Redis     map[string]RedisConfig    `yaml:"redis,omitempty"`
}

// AppConfig handles global system runtime context and environment modes.
type AppConfig struct {
	Name        string `yaml:"name" env:"APP_NAME" env-default:"fehmi-gateway"`
	Version     string `yaml:"version" env:"APP_VERSION" env-default:"1.0.0"`
	Environment string `yaml:"environment" env:"APP_ENV" env-default:"development"`
}

// LoggingConfig isolates file system targets for log aggregators/shipping workers.
type LoggingConfig struct {
	LogDir string `yaml:"log_dir" env:"LOG_DIR" env-default:"/var/log/fehmicorp"`
}

// ServerConfig defines ingress listeners (HTTP, gRPC, WebSocket routers).
type ServerConfig struct {
	Host            string `yaml:"host" env-default:"0.0.0.0"`
	Port            int    `yaml:"port" env-default:"8080"`
	Protocol        string `yaml:"protocol" env-default:"http"` // e.g., http, grpc, ws
	ReadTimeoutSec  int    `yaml:"read_timeout" env-default:"10"`
	WriteTimeoutSec int    `yaml:"write_timeout" env-default:"10"`
}

// ServicesConfig defines external downstream dependencies or sidecars.
type ServicesConfig struct {
	URL            string `yaml:"url"`
	TimeoutSec     int    `yaml:"timeout" env-default:"5"`
	MaxRetries     int    `yaml:"max_retries" env-default:"3"`
	CircuitBreaker bool   `yaml:"circuit_breaker" env-default:"false"`
}

// WorkerConfig holds execution contexts for async consumers (Kafka, RabbitMQ, Cron Engine tasks).
type WorkerConfig struct {
	Topic       string `yaml:"topic"`
	Group       string `yaml:"group"`
	Concurrency int    `yaml:"concurrency" env-default:"5"`
	Enabled     bool   `yaml:"enabled" env-default:"true"`
}

// DatabaseConfig holds connection pool attributes for engine orchestration.
type DatabaseConfig struct {
	Engine   string `yaml:"engine" env-default:"postgres"` // e.g., postgres, mongos, redis
	Host     string `yaml:"host" env-default:"localhost"`
	Port     int    `yaml:"port" env-default:"5432"`
	User     string `yaml:"user" env-default:"postgres"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	SSLMode  bool   `yaml:"ssl_mode" env-default:"false"`
	MaxConn  int    `yaml:"max_conn" env-default:"10"`
	MaxIdle  int    `yaml:"max_idle" env-default:"5"`
}

// RedisConfig maps custom attributes for data persistence caches or pipeline brokers.
type RedisConfig struct {
	Host     string `yaml:"host" env-default:"localhost"`
	Port     int    `yaml:"port" env-default:"6379"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db" env-default:"0"`
}

// MustLoad strictly initializes configurations, breaking early if context dependencies fail.
func MustLoad() *Config {
	var configPath string
	configPath = os.Getenv("CONFIG_PATH")

	if configPath == "" {
		flags := flag.String("config", "", "path to the configuration file")
		flag.Parse()
		configPath = *flags

		if configPath == "" {
			log.Fatal("Critical: Configuration path contextual variable is completely unassigned.")
		}
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("Critical: Target config file target map missing at location path: %s", configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("Critical: Failed to decode dynamic enterprise config map structure: %s", err.Error())
	}

	return &cfg
}

// Load executes a file-first fallback approach if preferred over panicking initialization styles.
func Load(path string) (*Config, error) {
	var cfg Config
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			if err := cleanenv.ReadConfig(path, &cfg); err != nil {
				return nil, fmt.Errorf("failed to process configuration file structure: %w", err)
			}
			return &cfg, nil
		}
	}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, fmt.Errorf("failed to extract fallback environment map values: %w", err)
	}
	return &cfg, nil
}

// InitLogger safely wires the runtime instance to write structured JSON files
// matching the pattern: LOG_DIR/Name.version.log
func (c *Config) InitLogger() (*os.File, error) {
	// Ensure log path namespace directory tree physically exists
	if err := os.MkdirAll(c.Logging.LogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to initialize logging destination directory: %w", err)
	}

	// Generate target logging identity profile layout: name.version.log
	logFileName := fmt.Sprintf("%s.%s.log", c.App.Name, c.App.Version)
	fullLogPath := filepath.Join(c.Logging.LogDir, logFileName)

	// Establish long-lived write pipeline handle
	file, err := os.OpenFile(fullLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open application log transaction stream file: %w", err)
	}

	// Configure default log outputs to broadcast cleanly to stdout and the local worker target file simultaneously
	multiWriter := io.MultiWriter(os.Stdout, file)
	log.SetOutput(multiWriter)

	// Strip out tracking timestamps from standard logging as the downstream custom worker will be tracking JSON event frames natively
	log.SetFlags(0)

	return file, nil
}

// Helper Accessors to shield runtime execution states from nil pointer panic states
func (c *Config) GetServer(name string) (ServerConfig, bool) {
	cfg, ok := c.Servers[name]
	return cfg, ok
}

func (c *Config) GetDatabase(name string) (DatabaseConfig, bool) {
	cfg, ok := c.Databases[name]
	return cfg, ok
}

func (c *Config) GetWorker(name string) (WorkerConfig, bool) {
	cfg, ok := c.Workers[name]
	return cfg, ok
}

type LogPayload struct {
	Timestamp string `json:"timestamp"`
	App       string `json:"app"`
	Version   string `json:"version"`
	TaskName  string `json:"task_name"`
	Status    string `json:"status"`
	Message   string `json:"message"`
}

func logTask(cfg *Config, taskName, status, message string) {
	payload := LogPayload{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		App:       cfg.App.Name,
		Version:   cfg.App.Version,
		TaskName:  taskName,
		Status:    status,
		Message:   message,
	}

	bytes, err := json.Marshal(payload)
	if err == nil {
		log.Println(string(bytes))
	}
}
