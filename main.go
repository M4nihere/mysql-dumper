package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// Config structure
type Config struct {
	MySQL struct {
		Host      string   `yaml:"host"`
		Port      int      `yaml:"port"`
		Username  string   `yaml:"username"`
		Password  string   `yaml:"password"`
		Databases []string `yaml:"databases"`
	} `yaml:"mysql"`
	Backup struct {
		BaseDir       string `yaml:"base_dir"`
		RunsPerDay    int    `yaml:"runs_per_day"`
		RetentionDays int    `yaml:"retention_days"`
		Threads       int    `yaml:"threads"`
		TimeoutSecs   int    `yaml:"timeout_seconds"`
	} `yaml:"backup"`
	Agent struct {
		StatusDB string `yaml:"status_db"` // path to sqlite file
	} `yaml:"agent"`
}

// Status record
type BackupStatus struct {
	ID        int64
	StartedAt time.Time
	EndedAt   time.Time
	Status    string // running / success / failed
	Error     string
	Dir       string
	SizeBytes int64
	Duration  int64
}

var (
	cfgPath = flag.String("config", "/etc/backup-agent/config.yaml", "Path to config YAML")
	debug   = flag.Bool("debug", false, "Enable debug logging")
)

func main() {
	flag.Parse()

	// logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})

	log.Info().Msg("backup-agent starting")

	// load config
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	// validate config
	if err := validateConfig(cfg); err != nil {
		log.Fatal().Err(err).Msg("invalid configuration")
	}

	// ensure base dir exists
	if err := os.MkdirAll(cfg.Backup.BaseDir, 0o755); err != nil {
		log.Fatal().Err(err).Msg("failed to create base backup dir")
	}

	// open status DB
	statusDBPath := cfg.Agent.StatusDB
	if statusDBPath == "" {
		statusDBPath = filepath.Join(cfg.Backup.BaseDir, "status.db")
	}
	db, err := sql.Open("sqlite3", statusDBPath+"?_busy_timeout=5000")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open status DB")
	}
	defer db.Close()

	if err := ensureStatusTable(db); err != nil {
		log.Fatal().Err(err).Msg("failed to setup status DB")
	}

	// validate mydumper present
	if err := checkMyDumper(); err != nil {
		log.Fatal().Err(err).Msg("mydumper check failed")
	}

	// validate mysql connectivity quickly
	if err := validateMySQLConn(cfg); err != nil {
		log.Fatal().Err(err).Msg("mysql connectivity failed")
	}

	// scheduler interval
	interval := calculateInterval(cfg.Backup.RunsPerDay)
	log.Info().
		Str("base_dir", cfg.Backup.BaseDir).
		Int("runs_per_day", cfg.Backup.RunsPerDay).
		Str("interval", interval.String()).
		Msg("configured scheduler")

	// context + graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// capture signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// run initial immediate backup then loop
	runTicker := time.NewTicker(interval)
	defer runTicker.Stop()

	// run once at startup immediately
	go func() {
		if err := runBackupCycle(ctx, db, cfg); err != nil {
			log.Error().Err(err).Msg("initial backup cycle failed")
		}
	}()

loop:
	for {
		select {
		case <-runTicker.C:
			// scheduled run
			go func() {
				if err := runBackupCycle(ctx, db, cfg); err != nil {
					log.Error().Err(err).Msg("scheduled backup cycle failed")
				}
			}()
		case sig := <-sigCh:
			log.Info().Str("signal", sig.String()).Msg("received shutdown")
			break loop
		case <-ctx.Done():
			log.Info().Msg("context canceled")
			break loop
		}
	}

	// shutdown
	log.Info().Msg("backup-agent exiting")
}

// loadConfig reads YAML config
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	// defaults
	if c.Backup.Threads == 0 {
		c.Backup.Threads = 4
	}
	if c.Backup.TimeoutSecs == 0 {
		c.Backup.TimeoutSecs = 3600
	}
	if c.Backup.RunsPerDay <= 0 {
		c.Backup.RunsPerDay = 1
	}
	if c.Backup.RetentionDays <= 0 {
		c.Backup.RetentionDays = 7
	}
	return &c, nil
}

func validateConfig(c *Config) error {
	if c.MySQL.Host == "" {
		return errors.New("mysql.host required")
	}
	if c.MySQL.Port == 0 {
		return errors.New("mysql.port required")
	}
	if c.MySQL.Username == "" {
		return errors.New("mysql.username required")
	}
	if c.MySQL.Password == "" {
		return errors.New("mysql.password required")
	}
	if len(c.MySQL.Databases) == 0 {
		return errors.New("mysql.databases required")
	}
	if c.Backup.BaseDir == "" {
		return errors.New("backup.base_dir required")
	}
	return nil
}

func calculateInterval(runsPerDay int) time.Duration {
	if runsPerDay <= 0 {
		runsPerDay = 1
	}
	secs := 24 * 3600 / runsPerDay
	return time.Duration(secs) * time.Second
}

func ensureStatusTable(db *sql.DB) error {
	q := `
CREATE TABLE IF NOT EXISTS backup_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at DATETIME,
    ended_at DATETIME,
    status TEXT,
    error TEXT,
    dir TEXT,
    size_bytes INTEGER,
    duration_secs INTEGER
);
`
	_, err := db.Exec(q)
	return err
}

func checkMyDumper() error {
	cmd := exec.Command("mydumper", "--version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mydumper not found: %w (output: %s)", err, string(out))
	}
	log.Info().Str("mydumper_version", strings.TrimSpace(string(out))).Msg("mydumper detected")
	return nil
}

func validateMySQLConn(cfg *Config) error {
	// quick ping using mysql client if available else skip -- we won't open DB here
	// just attempt a TCP connection by invoking mysql via --execute "SELECT 1"
	// but this may leak password on ps, so skip: instead rely on mydumper later — keep simple
	log.Debug().Msg("skipping explicit mysql connectivity test (mydumper will connect)")
	return nil
}

// runBackupCycle runs one backup across configured DBs
func runBackupCycle(ctx context.Context, db *sql.DB, cfg *Config) error {
	start := time.Now()
	statusID, err := insertStatus(db, start)
	if err != nil {
		log.Error().Err(err).Msg("failed to insert status")
	}

	// compute next steps per DB: we create year/month/dbname/timestamp folders
	totalSize := int64(0)
	var lastErr error

	// build escaped db list (used for information/logging if needed)
	escaped := make([]string, 0, len(cfg.MySQL.Databases))
	for _, d := range cfg.MySQL.Databases {
		if t := strings.TrimSpace(d); t != "" {
			escaped = append(escaped, regexp.QuoteMeta(t))
		}
	}
	// NOTE: we intentionally do per-DB runs using thisRegex below.
	// the aggregated pattern (if needed) is not required here.

	for _, dbname := range cfg.MySQL.Databases {
		// build path: base/YYYY/MM/dbname/YYYY-MM-DD_HH-MM-SS
		now := time.Now()
		year := now.Format("2006")
		month := now.Format("01")
		ts := now.Format("2006-01-02_15-04-05")
		backupDir := filepath.Join(cfg.Backup.BaseDir, year, month, dbname, ts)
		if err := os.MkdirAll(backupDir, 0o755); err != nil {
			lastErr = fmt.Errorf("mkdir failed for %s: %w", backupDir, err)
			log.Error().Err(lastErr).Msg("failed to create backup dir")
			continue
		}

		// run mydumper restricted to this DB using regex: ^(dbname)\..*
		// Note: we supply a regex that matches only the single db's tables to avoid dumping others
		thisRegex := fmt.Sprintf("^(%s)\\..*", regexp.QuoteMeta(dbname))

		// context with timeout for this db's run
		runCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Backup.TimeoutSecs)*time.Second)
		err := runMydumper(runCtx, cfg, backupDir, thisRegex)
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("mydumper failed for %s: %w", dbname, err)
			log.Error().Err(lastErr).Str("db", dbname).Msg("mydumper run failed")
			// continue to next DB, but record failure
			continue
		}

		// verify backup and compute size
		size, vfErr := computeAndVerify(backupDir)
		if vfErr != nil {
			lastErr = fmt.Errorf("verification failed for %s: %w", dbname, vfErr)
			log.Error().Err(lastErr).Str("db", dbname).Msg("verification failed")
			continue
		}
		totalSize += size

		log.Info().
			Str("db", dbname).
			Str("dir", backupDir).
			Int64("size_bytes", size).
			Msg("backup finished for db")
		// record run row for this DB as success
		if _, err := db.Exec(
			`UPDATE backup_runs SET ended_at = ?, status = ?, error = ?, dir = ?, size_bytes = ?, duration_secs = ? WHERE id = ?`,
			time.Now(), "success", "", backupDir, size, int64(time.Since(start).Seconds()), statusID,
		); err != nil {
			log.Error().Err(err).Msg("failed to update status row")
		}

		// run retention cleanup for db folder
		if err := runRetention(cfg, dbname); err != nil {
			log.Warn().Err(err).Str("db", dbname).Msg("retention cleanup failed")
		}
	}

	// If lastErr set and at least one DB had success, mark overall as failed (we record per-db above)
	finalStatus := "success"
	if lastErr != nil {
		finalStatus = "failed"
	}

	// finalize overall record (for the last inserted row)
	if _, err := db.Exec(
		`UPDATE backup_runs SET ended_at = ?, status = ?, error = ?, size_bytes = ?, duration_secs = ? WHERE id = ?`,
		time.Now(), finalStatus, maybeErrString(lastErr), totalSize, int64(time.Since(start).Seconds()), statusID,
	); err != nil {
		log.Error().Err(err).Msg("failed to finalize status row")
	}

	return lastErr
}

func insertStatus(db *sql.DB, started time.Time) (int64, error) {
	res, err := db.Exec(`INSERT INTO backup_runs (started_at, status) VALUES (?, ?)`, started, "running")
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func maybeErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func runMydumper(ctx context.Context, cfg *Config, outDir string, regex string) error {
	// build args
	args := []string{
		"--host", cfg.MySQL.Host,
		"--port", fmt.Sprintf("%d", cfg.MySQL.Port),
		"--user", cfg.MySQL.Username,
		"--password", cfg.MySQL.Password,
		"--outputdir", outDir,
		"--threads", fmt.Sprintf("%d", cfg.Backup.Threads),
		"--compress",
		"--verbose", "3",
		"--regex", regex,
		"--trx-consistency-only",
	}
	log.Info().Strs("args", args).Str("out", outDir).Msg("running mydumper")

	cmd := exec.CommandContext(ctx, "mydumper", args...)
	// stream stdout/stderr to logs
	cmd.Stdout = &logWriter{prefix: "mydumper"}
	cmd.Stderr = &logWriter{prefix: "mydumper"}

	start := time.Now()
	if err := cmd.Run(); err != nil {
		// check if timeout
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout after %d seconds", cfg.Backup.TimeoutSecs)
		}
		return fmt.Errorf("mydumper command failed: %w", err)
	}
	log.Info().Dur("elapsed", time.Since(start)).Msg("mydumper finished")
	return nil
}

// computeAndVerify walks folder, ensures actual dump files exist and returns total size
func computeAndVerify(dir string) (int64, error) {
	var total int64
	var found bool

	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		// skip metadata folder but still allow other files
		rel, _ := filepath.Rel(dir, p)
		if rel == "metadata" || strings.HasPrefix(rel, "metadata"+string(os.PathSeparator)) {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := strings.ToLower(d.Name())
		if strings.Contains(name, ".sql") || strings.Contains(name, "-schema") || strings.HasSuffix(name, ".zst") || strings.HasSuffix(name, ".gz") {
			found = true
			if fi, err := d.Info(); err == nil {
				total += fi.Size()
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk error: %w", err)
	}
	if !found {
		return 0, errors.New("no dump files found (only metadata present)")
	}
	return total, nil
}

func runRetention(cfg *Config, dbname string) error {
	// delete directories older than retention in: base/YYYY/MM/dbname/*
	cutoff := time.Now().AddDate(0, 0, -cfg.Backup.RetentionDays)
	root := filepath.Join(cfg.Backup.BaseDir)
	// walk YYYY/MM/dbname directories and remove older
	return filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			return nil
		}
		// check end path parts
		// we only care about paths that end with /dbname/<timestamp>
		parts := strings.Split(filepath.ToSlash(p), "/")
		if len(parts) < 4 {
			return nil
		}
		if parts[len(parts)-2] != dbname {
			return nil
		}
		// last part should be timestamp like 2006-01-02_15-04-05
		ts := parts[len(parts)-1]
		t, err := time.Parse("2006-01-02_15-04-05", ts)
		if err != nil {
			// not in timestamp format -> skip
			return nil
		}
		if t.Before(cutoff) {
			// remove this directory
			log.Info().Str("remove", p).Msg("removing old backup due retention")
			if err := os.RemoveAll(p); err != nil {
				log.Error().Err(err).Str("path", p).Msg("failed to remove old backup")
			}
		}
		return nil
	})
}

// simple log writer to stream external command output into zerolog
type logWriter struct {
	prefix string
}

func (lw *logWriter) Write(p []byte) (n int, err error) {
	s := strings.TrimSpace(string(p))
	if s == "" {
		return len(p), nil
	}
	log.Info().Str("src", lw.prefix).Msg(s)
	return len(p), nil
}
