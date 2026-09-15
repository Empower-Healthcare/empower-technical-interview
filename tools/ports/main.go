// Command ports picks free host ports for the Compose stack and records them
// in .env.
//
//	go run ./tools/ports          choose, update .env, print the result
//	go run ./tools/ports -check   print the result, change nothing, exit 1 on conflicts
//
// `make up` runs this before `docker compose up`. For every *_PORT variable in
// .env.example the desired value is whatever .env says, else the template's
// default. A desired port is kept when nothing on 127.0.0.1 has it, or when
// the listener is one of this project's own containers (a re-run of `make up`).
// Otherwise the next free port above it is chosen and written to .env, which
// Docker Compose and the Makefile both read. .env is the only place a port may
// be configured: a *_PORT variable exported in the shell (or passed as
// `make up KEY=...`) would reach Compose with its old value after this tool
// rewrote .env, so such variables are rejected with an explanation.
//
// Why a pre-check rather than Compose's automatic host-port allocation: Kafka
// advertises HOST://127.0.0.1:${KAFKA_HOST_PORT} to host clients and must know
// that port before the broker starts. The check-then-bind window is a race in
// theory; on a laptop it is fine, and Compose still fails loudly if it loses.
//
// The compose command comes from $COMPOSE (default "docker compose") so Podman
// users can point it at `podman compose`.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	host        = "127.0.0.1"
	envFile     = ".env"
	templFile   = ".env.example"
	searchLimit = 200
	psTimeout   = 30 * time.Second
	probeWait   = 300 * time.Millisecond
)

func main() {
	checkOnly := flag.Bool("check", false, "report only; exit 1 if any port is taken")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	code, err := run(ctx, os.Stdout, *checkOnly)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ports:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(ctx context.Context, out io.Writer, checkOnly bool) (int, error) {
	root, err := repoRoot()
	if err != nil {
		return 1, err
	}
	specs, err := readSpecs(filepath.Join(root, templFile))
	if err != nil {
		return 1, err
	}
	if err := rejectEnvironmentPorts(specs, os.LookupEnv); err != nil {
		return 1, err
	}
	existing, overrides, err := readEnv(filepath.Join(root, envFile))
	if err != nil {
		return 1, err
	}
	ours, err := publishedPorts(ctx, root, composeCommand())
	if err != nil {
		// No Compose, or the daemon is down: every desired port is then judged
		// purely by whether it can be bound, which is still correct.
		fmt.Fprintf(os.Stderr, "ports: could not list this project's containers (%v); treating none as ours\n", err)
	}

	choices, err := Choose(specs, Selection{Overrides: overrides, Ours: ours, Free: prober(ctx), Limit: searchLimit})
	if err != nil {
		return 1, err
	}
	printChoices(out, choices)

	conflicts := 0
	for _, c := range choices {
		if c.Changed() {
			conflicts++
		}
	}
	if checkOnly {
		if conflicts > 0 {
			fmt.Fprintf(out, "%d port(s) in use; `make up` will pick the alternatives above\n", conflicts)
			return 1, nil
		}
		fmt.Fprintln(out, "all ports available")
		return 0, nil
	}
	if conflicts == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 1, err
	}
	if existing == "" {
		// First write: start from the template so the file explains itself.
		tmpl, err := os.ReadFile(filepath.Join(root, templFile))
		if err != nil {
			return 1, err
		}
		existing = string(tmpl)
	}
	if err := os.WriteFile(filepath.Join(root, envFile), []byte(RenderEnv(existing, choices)), 0o644); err != nil {
		return 1, err
	}
	fmt.Fprintf(out, "wrote %d alternative port(s) to %s; `make urls` shows the result\n", conflicts, envFile)
	return 0, nil
}

// repoRoot is the directory holding .env.example: the current directory when
// run via make, or the nearest ancestor otherwise.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, templFile)); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("%s not found in %s or any parent", templFile, dir)
		}
		dir = parent
	}
}

// rejectEnvironmentPorts fails when any known port variable is set in the
// process environment. Compose gives the environment precedence over .env, so
// a value there would silently undo whatever this tool writes.
func rejectEnvironmentPorts(specs []PortSpec, lookup func(string) (string, bool)) error {
	for _, s := range specs {
		if v, ok := lookup(s.Name); ok {
			return fmt.Errorf("%s=%q is set in the environment; ports are configured only in %s (unset it, or move the value there)", s.Name, v, envFile)
		}
	}
	return nil
}

func readSpecs(path string) ([]PortSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	specs, err := ParseSpecs(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("%s defines no *_PORT variables", path)
	}
	return specs, nil
}

// readEnv returns the raw .env content ("" when absent) and its active ports.
func readEnv(path string) (string, map[string]int, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", map[string]int{}, nil
	}
	if err != nil {
		return "", nil, err
	}
	overrides, err := ParseOverrides(strings.NewReader(string(data)))
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", path, err)
	}
	return string(data), overrides, nil
}

func composeCommand() []string {
	if v := strings.TrimSpace(os.Getenv("COMPOSE")); v != "" {
		return strings.Fields(v)
	}
	return []string{"docker", "compose"}
}

// publishedPorts asks Compose which 127.0.0.1 ports this project's containers
// publish, so a running stack is not mistaken for a conflict.
func publishedPorts(ctx context.Context, dir string, compose []string) (map[int]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, psTimeout)
	defer cancel()
	args := append(compose[1:], "ps", "--format", "json")
	cmd := exec.CommandContext(ctx, compose[0], args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return map[int]bool{}, err
	}
	return parsePublished(out)
}

type composeContainer struct {
	Publishers []struct {
		URL           string `json:"URL"`
		PublishedPort int    `json:"PublishedPort"`
	} `json:"Publishers"`
}

// parsePublished accepts both shapes Compose has printed: one JSON object per
// line, or a single JSON array.
func parsePublished(raw []byte) (map[int]bool, error) {
	text := strings.TrimSpace(string(raw))
	ports := map[int]bool{}
	if text == "" {
		return ports, nil
	}
	var containers []composeContainer
	if strings.HasPrefix(text, "[") {
		if err := json.Unmarshal([]byte(text), &containers); err != nil {
			return nil, err
		}
	} else {
		for _, line := range strings.Split(text, "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var c composeContainer
			if err := json.Unmarshal([]byte(line), &c); err != nil {
				return nil, err
			}
			containers = append(containers, c)
		}
	}
	for _, c := range containers {
		for _, p := range c.Publishers {
			if p.URL == host && p.PublishedPort != 0 {
				ports[p.PublishedPort] = true
			}
		}
	}
	return ports, nil
}

// prober returns the Free function for Choose: whether nothing accepts
// connections on 127.0.0.1:port. A connect probe is used rather than a trial
// bind: Go listeners set SO_REUSEADDR, which on BSD-derived systems (macOS) can
// let a bind succeed beside an existing wildcard listener that Docker would
// then collide with. A successful connect is unambiguous, a refused one means
// the port is free, and anything else (timeout, cancellation, permission) is
// reported as an error rather than guessed.
func prober(ctx context.Context) func(port int) (bool, error) {
	dialer := net.Dialer{Timeout: probeWait}
	return func(port int) (bool, error) {
		addr := net.JoinHostPort(host, fmt.Sprint(port))
		conn, err := dialer.DialContext(ctx, "tcp4", addr)
		switch {
		case err == nil:
			_ = conn.Close()
			return false, nil
		case errors.Is(err, syscall.ECONNREFUSED):
			return true, nil
		case ctx.Err() != nil:
			return false, ctx.Err()
		default:
			return false, fmt.Errorf("probing %s: %w", addr, err)
		}
	}
}

func printChoices(out io.Writer, choices []Choice) {
	width := 0
	for _, c := range choices {
		width = max(width, len(c.Name))
	}
	for _, c := range choices {
		var note string
		switch c.Reason {
		case ReasonFree:
			note = "free"
		case ReasonOurs:
			note = "published by this stack"
		case ReasonTaken:
			note = fmt.Sprintf("in use by another process -> %d", c.Chosen)
		}
		fmt.Fprintf(out, "  %-*s  %-5d %s\n", width, c.Name, c.Desired, note)
	}
}
