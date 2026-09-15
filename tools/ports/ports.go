// Command ports picks free host ports for the Compose stack and records them
// in .env. See main.go for the command-line behaviour; this file is the pure
// part: parsing, selection, and rendering, with no I/O.
package main

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// assignment matches `KEY_PORT=1234` and its commented form `#KEY_PORT=1234`.
// Commented lines are how .env.example states defaults.
var assignment = regexp.MustCompile(`^\s*(#?)\s*([A-Z][A-Z0-9_]*_PORT)\s*=\s*(\d+)\s*$`)

// PortSpec is one published port: its .env variable and default value.
type PortSpec struct {
	Name    string
	Default int
}

// Reason records why a port was chosen.
type Reason uint8

const (
	// ReasonFree means nothing on 127.0.0.1 has the desired port.
	ReasonFree Reason = iota
	// ReasonOurs means the desired port is published by this Compose project already.
	ReasonOurs
	// ReasonTaken means another process holds the desired port; Chosen differs.
	ReasonTaken
)

// Choice is the outcome for one PortSpec.
type Choice struct {
	Name    string
	Desired int
	Chosen  int
	Reason  Reason
}

// Changed reports whether .env needs this choice written.
func (c Choice) Changed() bool { return c.Chosen != c.Desired }

// ParseSpecs reads a dotenv template and returns every *_PORT variable in file
// order, taking commented assignments as defaults.
func ParseSpecs(r io.Reader) ([]PortSpec, error) {
	var specs []PortSpec
	seen := map[string]bool{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		m := assignment.FindStringSubmatch(sc.Text())
		if m == nil || seen[m[2]] {
			continue
		}
		port, err := strconv.Atoi(m[3])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", m[2], err)
		}
		seen[m[2]] = true
		specs = append(specs, PortSpec{Name: m[2], Default: port})
	}
	return specs, sc.Err()
}

// ParseOverrides reads a dotenv file and returns the active (uncommented)
// *_PORT assignments.
func ParseOverrides(r io.Reader) (map[string]int, error) {
	overrides := map[string]int{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		m := assignment.FindStringSubmatch(sc.Text())
		if m == nil || m[1] == "#" {
			continue
		}
		port, err := strconv.Atoi(m[3])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", m[2], err)
		}
		overrides[m[2]] = port
	}
	return overrides, sc.Err()
}

// Selection is everything Choose needs besides the specs.
type Selection struct {
	Overrides map[string]int // active .env values, win over defaults
	Ours      map[int]bool   // host ports this Compose project already publishes
	// Free reports whether nothing listens on the port. An error aborts the
	// selection: a probe that could not decide must not be read as "free".
	Free  func(port int) (bool, error)
	Limit int // how far above a taken port to search
}

const maxPort = 65535

// Choose decides a host port for every spec. A desired port is kept when it is
// free or already ours; otherwise the next free port above it is used, skipping
// ports that other specs want so one swap cannot steal a neighbour's default.
// Desired ports must be valid and distinct: two services cannot publish on the
// same host port, and guessing which one should move would hide a
// misconfiguration in .env.
func Choose(specs []PortSpec, sel Selection) ([]Choice, error) {
	desired, err := desiredPorts(specs, sel.Overrides)
	if err != nil {
		return nil, err
	}
	reserved := make(map[int]bool, len(specs))
	for _, want := range desired {
		reserved[want] = true
	}

	choices := make([]Choice, 0, len(specs))
	for _, s := range specs {
		want := desired[s.Name]
		if sel.Ours[want] {
			choices = append(choices, Choice{Name: s.Name, Desired: want, Chosen: want, Reason: ReasonOurs})
			continue
		}
		isFree, err := sel.Free(want)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.Name, err)
		}
		if isFree {
			choices = append(choices, Choice{Name: s.Name, Desired: want, Chosen: want, Reason: ReasonFree})
			continue
		}
		alt, err := nextFree(want, reserved, sel)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.Name, err)
		}
		reserved[alt] = true
		choices = append(choices, Choice{Name: s.Name, Desired: want, Chosen: alt, Reason: ReasonTaken})
	}
	return choices, nil
}

// desiredPorts applies overrides to defaults and rejects out-of-range or
// duplicate values, naming both variables when two want the same port.
func desiredPorts(specs []PortSpec, overrides map[string]int) (map[string]int, error) {
	desired := make(map[string]int, len(specs))
	owner := make(map[int]string, len(specs))
	for _, s := range specs {
		want := s.Default
		if v, ok := overrides[s.Name]; ok {
			want = v
		}
		if want < 1 || want > maxPort {
			return nil, fmt.Errorf("%s=%d: port must be between 1 and %d", s.Name, want, maxPort)
		}
		if other, dup := owner[want]; dup {
			return nil, fmt.Errorf("%s and %s both want port %d; give one of them a different value in .env", other, s.Name, want)
		}
		owner[want] = s.Name
		desired[s.Name] = want
	}
	return desired, nil
}

func nextFree(start int, reserved map[int]bool, sel Selection) (int, error) {
	for port := start + 1; port <= start+sel.Limit && port <= maxPort; port++ {
		if reserved[port] {
			continue
		}
		isFree, err := sel.Free(port)
		if err != nil {
			return 0, err
		}
		if isFree {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no free port within %d above %d", sel.Limit, start)
}

// RenderEnv returns the dotenv content with every changed choice set as an
// active KEY=port line. Existing lines (including a commented default for the
// same key) are replaced in place; keys with no line are appended in name
// order. Everything else is preserved byte for byte.
func RenderEnv(existing string, choices []Choice) string {
	updates := map[string]int{}
	for _, c := range choices {
		if c.Changed() {
			updates[c.Name] = c.Chosen
		}
	}
	if len(updates) == 0 {
		return existing
	}

	lines := strings.Split(strings.TrimRight(existing, "\n"), "\n")
	if existing == "" {
		lines = nil
	}
	written := map[string]bool{}
	for i, line := range lines {
		m := assignment.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if port, ok := updates[m[2]]; ok {
			lines[i] = fmt.Sprintf("%s=%d", m[2], port)
			written[m[2]] = true
		}
	}
	var missing []string
	for name := range updates {
		if !written[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	for _, name := range missing {
		lines = append(lines, fmt.Sprintf("%s=%d", name, updates[name]))
	}
	return strings.Join(lines, "\n") + "\n"
}
