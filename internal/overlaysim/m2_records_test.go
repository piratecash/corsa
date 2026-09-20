package overlaysim

// m2_records_test.go keeps what the M2 report cannot: the result of EVERY pair,
// in both graphs, as a file that outlives the run.
//
// Why it exists (P2 of 2026-09-19). The driver printed medians, percentiles and
// outcome shares, and dropped `ByPair` when the iteration moved on. §5.3 says
// the experimental hop limit L is chosen AFTER the lengths are seen, and
// underHopLimit recomputes any L from a finished run — but only from the
// per-pair record. From aggregates it cannot be done at all: a median of four
// hops says nothing about how many pairs sat at nine. So a sweep that kept only
// the aggregates had already destroyed the thing that made choosing L cheap, and
// every candidate L would have meant re-running the sweep.
//
// What a record therefore carries, and why each field is not optional:
//
//	outcome  — success / dead end / no path / budget spent, kept apart;
//	hops     — the transitions actually performed, which is what a limit cuts;
//	stopped  — where the walk ended. ⚠️ underHopLimit preserves it for a pair the
//	           limit did not touch and refuses to invent it for one it did, so a
//	           record without it could not reproduce an untouched pair exactly.
//
// Plus the parameters and the version: a file of numbers whose shape, seed,
// rule, quota, sample and code version are not in it is not evidence of
// anything, and this is the same lesson the round-34 manifest taught about
// naming only part of what was measured.

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// m2RecordFormat is the format tag written as the first line. It changes when
// the columns change, and a reader that does not know the tag refuses the file
// rather than guessing which column is which.
const m2RecordFormat = "m2-records/v1"

// m2RunKey is everything needed to say WHICH measurement a file holds.
type m2RunKey struct {
	Shape  string
	Nodes  int
	Degree int
	Budget int
	Policy string
	Quota  int
	Seed   uint64

	// PairsRequested and PairSeed identify the sample; Eligible says how large
	// the population it was drawn from was, so a short sample is visible in the
	// file itself and not only in the log.
	PairsRequested int
	PairSeed       uint64
	Eligible       int

	// Sources is the digest of the sources the run was made with, passed in by
	// the operator (M2_SOURCES). ⚠️ "unstamped" is written when nothing was
	// passed: an absent version must read as absent, never as "current".
	Sources string
}

func (k m2RunKey) String() string {
	return fmt.Sprintf("%s policy=%s quota=%d seed=%d", k.Shape, k.Policy, k.Quota, k.Seed)
}

// m2PairRecord is one pair measured in both graphs.
type m2PairRecord struct {
	Index          int
	Source, Target int32
	Full, Half     routingResult
}

// m2Records is a file's worth: the key, and one record per pair in sample order.
type m2Records struct {
	Key   m2RunKey
	Pairs []m2PairRecord
}

// pairRecordsOf joins the sample with the two measurements. ⚠️ The join key is
// the POSITION in the sample, which is the same key compareLengths uses; a
// record built by any other correspondence would compare two graphs on pairs
// only one of them was asked about.
func pairRecordsOf(sample m2PairSample, full, half routingReport) []m2PairRecord {
	records := make([]m2PairRecord, 0, len(sample.Pairs))
	for index, pair := range sample.Pairs {
		records = append(records, m2PairRecord{
			Index:  index,
			Source: pair[0],
			Target: pair[1],
			Full:   full.ByPair[index],
			Half:   half.ByPair[index],
		})
	}
	return records
}

// outcomeToken and outcomeFromToken are the on-disk spelling. String() is for
// humans and contains spaces; a column must not.
func outcomeToken(o routingOutcome) string {
	switch o {
	case routingSuccess:
		return "success"
	case routingDeadEnd:
		return "dead_end"
	case routingNoPath:
		return "no_path"
	case routingBudgetSpent:
		return "budget_spent"
	default:
		return "unknown"
	}
}

func outcomeFromToken(token string) (routingOutcome, error) {
	switch token {
	case "success":
		return routingSuccess, nil
	case "dead_end":
		return routingDeadEnd, nil
	case "no_path":
		return routingNoPath, nil
	case "budget_spent":
		return routingBudgetSpent, nil
	default:
		return 0, fmt.Errorf("%q is not an outcome this format knows", token)
	}
}

// m2RecordColumns is the column header, written by the writer and required
// EXACTLY by the reader.
//
// ⚠️ Requiring it exactly, rather than checking that the line starts with
// "pair source target", is the P2 of 2026-09-19. The two graphs are told apart
// by nothing but column position, so a file whose `full_*` and `half_*` groups
// were swapped together with their data passed a prefix check and was then read
// with the two graphs exchanged — the structural half reported as the full
// network and back.
//
// ⚠️ And the headline ratio does NOT catch it: on the reference fixture M2-G
// reads 1.00 both ways (m2_records_reference_test.go logs both), because it is a
// ratio of two medians that merely change places. What moves is the success and
// dead-end counts per graph — that is, the numbers §3 of the report draws its
// conclusion from.
const m2RecordColumns = "pair\tsource\ttarget\tfull_outcome\tfull_hops\tfull_stopped\t" +
	"half_outcome\thalf_hops\thalf_stopped"

// m2RecordFileName is deterministic: the same run key always names the same
// file, so a directory of records can be joined to a report by key rather than
// by reading every file. ⚠️ That determinism is also why writeM2Records refuses
// to overwrite — a rerun lands on exactly these names.
func m2RecordFileName(key m2RunKey) string {
	policy := strings.NewReplacer("/", "-", " ", "-").Replace(key.Policy)
	return fmt.Sprintf("m2-%s-%s-q%d-seed%d.tsv", key.Shape, policy, key.Quota, key.Seed)
}

// writeM2Records writes one measurement. The header is comment lines, so the
// body is a plain TSV any tool can read.
//
// ⚠️ It REFUSES to overwrite an existing file (P2 of 2026-09-19). The file name
// is derived from the run key, so a rerun of the same shape, rule, quota and
// seed lands on the same name — and a rerun started by accident silently
// replaced 160 presented records with unattributable ones. "Rerun into a
// separate directory" is now enforced here rather than remembered: an existing
// file means the operator is about to overwrite evidence, and only an explicit
// M2_RECORDS_OVERWRITE=1 allows it.
//
// ⚠️ The refusal is the OPEN ITSELF, not a check before it (P2 of 2026-09-19,
// second round). The first version asked os.Stat and then wrote: two runs
// started together both saw no file, both proceeded, and the second replaced the
// first's evidence without anyone passing the override. A sequential test cannot
// see that window at all. O_CREATE|O_EXCL closes it in the kernel — the name is
// created by exactly one caller — which is the same rule the project applies to
// files that must not be clobbered elsewhere in the tree.
func writeM2Records(dir string, records m2Records) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("records directory %s: %w", dir, err)
	}
	path := filepath.Join(dir, m2RecordFileName(records.Key))

	var out strings.Builder
	key := records.Key
	fmt.Fprintf(&out, "# %s\n", m2RecordFormat)
	fmt.Fprintf(&out, "# shape=%s nodes=%d degree=%d budget_B=%d\n",
		key.Shape, key.Nodes, key.Degree, key.Budget)
	fmt.Fprintf(&out, "# policy=%s quota=%d seed=%d\n", key.Policy, key.Quota, key.Seed)
	fmt.Fprintf(&out, "# pairs_requested=%d pair_seed=%d eligible=%d pairs_written=%d\n",
		key.PairsRequested, key.PairSeed, key.Eligible, len(records.Pairs))
	// ⚠️ The hop budget of the measurement is part of the evidence: a record
	// taken under a budget cannot be recomputed under another one, and the
	// reader enforces that rather than trusting the caller.
	fmt.Fprintf(&out, "# hop_budget=none sources=%s\n", key.Sources)
	fmt.Fprintf(&out, "%s\n", m2RecordColumns)

	for _, record := range records.Pairs {
		fmt.Fprintf(&out, "%d\t%d\t%d\t%s\t%d\t%d\t%s\t%d\t%d\n",
			record.Index, record.Source, record.Target,
			outcomeToken(record.Full.Outcome), record.Full.Hops, record.Full.Stopped,
			outcomeToken(record.Half.Outcome), record.Half.Hops, record.Half.Stopped)
	}

	// O_EXCL unless the operator insisted; then, and only then, the file may be
	// replaced — and O_TRUNC is what "replaced" has to mean, or a shorter record
	// would leave the tail of a longer one behind it.
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if strings.TrimSpace(os.Getenv("M2_RECORDS_OVERWRITE")) == "1" {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf(
				"%s already holds a record for %s: a rerun writes into a SEPARATE directory, so "+
					"that a measurement already presented cannot be replaced by accident "+
					"(M2_RECORDS_OVERWRITE=1 to insist)", path, records.Key)
		}
		return "", fmt.Errorf("creating %s: %w", path, err)
	}

	if _, writeErr := file.WriteString(out.String()); writeErr != nil {
		// ⚠️ The close error is deliberately dropped HERE and nowhere else: the
		// write already failed, and reporting the close instead would hide why.
		_ = file.Close()
		return "", fmt.Errorf("writing %s: %w", path, writeErr)
	}
	// ⚠️ Close is checked: on a filesystem that defers, this is where a full disk
	// or a broken mount surfaces, and a record that never reached the disk would
	// otherwise be reported as written.
	if closeErr := file.Close(); closeErr != nil {
		return "", fmt.Errorf("closing %s: %w", path, closeErr)
	}
	return path, nil
}

// readM2Records reads a file back. Every refusal below is a case where reading
// on would produce numbers that look fine and describe something else.
func readM2Records(path string) (m2Records, error) {
	file, err := os.Open(path)
	if err != nil {
		return m2Records{}, err
	}
	defer func() { _ = file.Close() }()

	return parseM2Records(file)
}

func parseM2Records(source io.Reader) (m2Records, error) {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	records := m2Records{}
	sawFormat, sawColumns, sawBudget := false, false, false
	written := -1

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "# ") {
			body := strings.TrimPrefix(line, "# ")
			if !sawFormat {
				if body != m2RecordFormat {
					return m2Records{}, fmt.Errorf(
						"this is not %s but %q — the columns may mean anything, so it is refused",
						m2RecordFormat, body)
				}
				sawFormat = true
				continue
			}
			for _, field := range strings.Fields(body) {
				name, value, ok := strings.Cut(field, "=")
				if !ok {
					continue
				}
				if err := records.Key.assign(name, value, &written, &sawBudget); err != nil {
					return m2Records{}, err
				}
			}
			continue
		}

		if !sawFormat {
			return m2Records{}, fmt.Errorf("the file does not start with the %s tag", m2RecordFormat)
		}
		if !sawColumns {
			// ⚠️ Equality, not a prefix: the two graphs are distinguished by
			// column POSITION alone, so a header whose groups were exchanged
			// would be read with the full network and the structural half
			// swapped — and every number downstream still looks like a number.
			if line != m2RecordColumns {
				return m2Records{}, fmt.Errorf(
					"the column header is missing, reordered or renamed, so which column belongs "+
						"to which graph is not established:\n  want %q\n  got  %q",
					m2RecordColumns, line)
			}
			sawColumns = true
			continue
		}

		record, err := parseM2PairLine(line)
		if err != nil {
			return m2Records{}, err
		}
		if record.Index != len(records.Pairs) {
			// Sample ORDER is the join key between the two graphs and between a
			// record and a rerun. A file whose indices skip or repeat cannot be
			// joined, and silently renumbering it would hide exactly that.
			return m2Records{}, fmt.Errorf(
				"pair index %d arrived at position %d — the sample order is broken",
				record.Index, len(records.Pairs))
		}
		records.Pairs = append(records.Pairs, record)
	}
	if err := scanner.Err(); err != nil {
		return m2Records{}, err
	}

	switch {
	case !sawFormat:
		return m2Records{}, fmt.Errorf("empty file: no %s tag", m2RecordFormat)
	case !sawColumns:
		return m2Records{}, fmt.Errorf("no column header, so no records")
	case !sawBudget:
		return m2Records{}, fmt.Errorf(
			"the file does not say under which hop budget it was measured; a record taken under " +
				"a budget cannot be recomputed under another one")
	case written >= 0 && written != len(records.Pairs):
		return m2Records{}, fmt.Errorf(
			"the header promises %d pairs and the body holds %d — the file is truncated",
			written, len(records.Pairs))
	}
	return records, nil
}

// assign fills one header field. Unknown fields are ignored on purpose: a newer
// writer may add one, and refusing a file over a field this reader does not use
// would be strictness without a reason.
func (k *m2RunKey) assign(name, value string, written *int, sawBudget *bool) error {
	atoi := func() (int, error) { return strconv.Atoi(value) }
	atou := func() (uint64, error) { return strconv.ParseUint(value, 10, 64) }

	var err error
	switch name {
	case "shape":
		k.Shape = value
	case "policy":
		k.Policy = value
	case "sources":
		k.Sources = value
	case "nodes":
		k.Nodes, err = atoi()
	case "degree":
		k.Degree, err = atoi()
	case "budget_B":
		k.Budget, err = atoi()
	case "quota":
		k.Quota, err = atoi()
	case "eligible":
		k.Eligible, err = atoi()
	case "pairs_requested":
		k.PairsRequested, err = atoi()
	case "pairs_written":
		*written, err = atoi()
	case "seed":
		k.Seed, err = atou()
	case "pair_seed":
		k.PairSeed, err = atou()
	case "hop_budget":
		if value != "none" {
			return fmt.Errorf(
				"the file was measured under hop budget %q: what its searches would have done "+
					"past that point is not in the record, so no other limit can be recomputed "+
					"from it", value)
		}
		*sawBudget = true
	}
	if err != nil {
		return fmt.Errorf("header field %s=%q: %w", name, value, err)
	}
	return nil
}

func parseM2PairLine(line string) (m2PairRecord, error) {
	columns := strings.Split(line, "\t")
	if len(columns) != 9 {
		return m2PairRecord{}, fmt.Errorf("a record needs nine columns, this one has %d: %q",
			len(columns), line)
	}

	number := func(text string) (int, error) { return strconv.Atoi(strings.TrimSpace(text)) }

	index, err := number(columns[0])
	if err != nil {
		return m2PairRecord{}, fmt.Errorf("pair index %q: %w", columns[0], err)
	}
	source, err := number(columns[1])
	if err != nil {
		return m2PairRecord{}, fmt.Errorf("source %q: %w", columns[1], err)
	}
	target, err := number(columns[2])
	if err != nil {
		return m2PairRecord{}, fmt.Errorf("target %q: %w", columns[2], err)
	}

	full, err := parseM2Side(columns[3], columns[4], columns[5])
	if err != nil {
		return m2PairRecord{}, fmt.Errorf("pair %d, full graph: %w", index, err)
	}
	half, err := parseM2Side(columns[6], columns[7], columns[8])
	if err != nil {
		return m2PairRecord{}, fmt.Errorf("pair %d, structural half: %w", index, err)
	}

	return m2PairRecord{
		Index:  index,
		Source: int32(source),
		Target: int32(target),
		Full:   full,
		Half:   half,
	}, nil
}

func parseM2Side(outcomeText, hopsText, stoppedText string) (routingResult, error) {
	outcome, err := outcomeFromToken(strings.TrimSpace(outcomeText))
	if err != nil {
		return routingResult{}, err
	}
	hops, err := strconv.Atoi(strings.TrimSpace(hopsText))
	if err != nil {
		return routingResult{}, fmt.Errorf("hops %q: %w", hopsText, err)
	}
	stopped, err := strconv.Atoi(strings.TrimSpace(stoppedText))
	if err != nil {
		return routingResult{}, fmt.Errorf("stopped %q: %w", stoppedText, err)
	}
	return routingResult{Outcome: outcome, Hops: hops, Stopped: int32(stopped)}, nil
}

// reportsFromRecords rebuilds the two routingReports a file was made from.
//
// ⚠️ Budget is set to noBudget because the header was checked to say so. That is
// what lets underHopLimit run on the result — and it is also why the reader
// refuses a file measured under a budget instead of quietly relabelling it.
func reportsFromRecords(records m2Records) (full, half routingReport) {
	build := func(pick func(m2PairRecord) routingResult) routingReport {
		report := routingReport{
			Pairs:    len(records.Pairs),
			Outcomes: map[routingOutcome]int{},
			ByPair:   make([]routingResult, 0, len(records.Pairs)),
			Budget:   noBudget,
		}
		hops := make([]int, 0, len(records.Pairs))
		for _, record := range records.Pairs {
			result := pick(record)
			report.Outcomes[result.Outcome]++
			report.ByPair = append(report.ByPair, result)
			if result.Outcome == routingSuccess {
				hops = append(hops, result.Hops)
			}
		}
		report.Lengths = summariseLengths(hops)
		return report
	}

	return build(func(r m2PairRecord) routingResult { return r.Full }),
		build(func(r m2PairRecord) routingResult { return r.Half })
}
