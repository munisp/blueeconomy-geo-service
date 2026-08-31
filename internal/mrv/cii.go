// CII computation (MARPOL Annex VI reg. 28; attained CII formula per
// MEPC.333(76) G2). EVERY parameter — the reference line (a, c) per ship
// type, the annual reduction factor z, the A-E rating boundary multipliers
// and the capacity basis — comes from an operator-approved, versioned,
// source-cited configuration document (MRV_CII_CONFIG_PATH). The coding
// agent transcribed nothing from memory: without an approved config the
// outcome is NOT_COMPUTABLE, never estimated.
package mrv

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"time"
)

// CIIConfigPathEnv carries the path of the operator-approved CII
// configuration document.
const CIIConfigPathEnv = "MRV_CII_CONFIG_PATH"

// CIIConfig is the operator-approved, versioned CII parameter document.
// Every numeric field is fixed-point nano (x1e9) rendered as a decimal
// string so the document itself never carries a float.
type CIIConfig struct {
	Version    string `json:"version"`
	ApprovedBy string `json:"approvedBy"`
	ApprovedAt string `json:"approvedAt"`
	// Citation of the governing resolutions, e.g. "MEPC.333(76) (G2);
	// MEPC.328(76); G1/G3/G4/G5 companion resolutions" plus the approving
	// instrument. Mandatory.
	Citation  string                    `json:"citation"`
	ShipTypes map[string]CIIShipTypeCfg `json:"shipTypes"`
}

// CIIShipTypeCfg carries the per-ship-type CII parameters.
type CIIShipTypeCfg struct {
	// CapacityField selects the capacity basis: "DWT" or "GT".
	CapacityField string `json:"capacityField"`
	// ReferenceLineANano / ReferenceLineCNano are the 2019 reference-line
	// parameters a and c (G1), fixed-point nano decimal strings.
	ReferenceLineANano string `json:"referenceLineANano"`
	ReferenceLineCNano string `json:"referenceLineCNano"`
	// ReductionFactorsZNano maps calendar year (e.g. "2026") to the annual
	// reduction factor z (G3), fixed-point nano decimal string.
	ReductionFactorsZNano map[string]string `json:"reductionFactorsZNano"`
	// RatingBoundariesNano are the four A-E rating boundary multipliers
	// exp(d1)..exp(d4) (G4) applied to the required CII, fixed-point nano
	// decimal strings, ascending.
	RatingBoundariesNano []string `json:"ratingBoundariesNano"`
	// Citation for this ship type's row, mandatory.
	Citation string `json:"citation"`
}

// LoadCIIConfig reads and validates the operator-approved CII config. A nil
// config (path unset) is the honest NOT_COMPUTABLE posture; a present but
// malformed document fails closed.
func LoadCIIConfig(path string) (*CIIConfig, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CII config %s: %w", path, err)
	}
	var config CIIConfig
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("CII config %s is not valid JSON: %w", path, err)
	}
	if strings.TrimSpace(config.Version) == "" || strings.TrimSpace(config.ApprovedBy) == "" ||
		strings.TrimSpace(config.Citation) == "" {
		return nil, errors.New("CII config must carry version, approvedBy and citation")
	}
	if len(config.ShipTypes) == 0 {
		return nil, errors.New("CII config must configure at least one ship type")
	}
	for shipType, entry := range config.ShipTypes {
		if entry.CapacityField != "DWT" && entry.CapacityField != "GT" {
			return nil, fmt.Errorf("CII config %s: capacityField must be DWT or GT", shipType)
		}
		if _, err := parseNano(entry.ReferenceLineANano); err != nil {
			return nil, fmt.Errorf("CII config %s referenceLineANano: %w", shipType, err)
		}
		if _, err := parseNano(entry.ReferenceLineCNano); err != nil {
			return nil, fmt.Errorf("CII config %s referenceLineCNano: %w", shipType, err)
		}
		if len(entry.ReductionFactorsZNano) == 0 {
			return nil, fmt.Errorf("CII config %s: reductionFactorsZNano is required", shipType)
		}
		for year, z := range entry.ReductionFactorsZNano {
			value, err := parseNano(z)
			if err != nil {
				return nil, fmt.Errorf("CII config %s year %s: %w", shipType, year, err)
			}
			if value >= 1_000_000_000 {
				return nil, fmt.Errorf("CII config %s year %s: z must be below 1", shipType, year)
			}
		}
		if len(entry.RatingBoundariesNano) != 4 {
			return nil, fmt.Errorf("CII config %s: exactly four rating boundary multipliers are required", shipType)
		}
		previous := uint64(0)
		for i, boundary := range entry.RatingBoundariesNano {
			value, err := parseNano(boundary)
			if err != nil {
				return nil, fmt.Errorf("CII config %s rating boundary %d: %w", shipType, i, err)
			}
			if i > 0 && value <= previous {
				return nil, fmt.Errorf("CII config %s: rating boundaries must be strictly ascending", shipType)
			}
			previous = value
		}
		if strings.TrimSpace(entry.Citation) == "" {
			return nil, fmt.Errorf("CII config %s: citation is required", shipType)
		}
	}
	return &config, nil
}

// parseNano parses a fixed-point nano decimal string ("0.474", "474000000"
// or "4.74e8" are NOT all accepted — only plain decimal with an optional
// fractional part of at most 9 digits) into the x1e9 integer.
func parseNano(text string) (uint64, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, errors.New("value is empty")
	}
	negative := strings.HasPrefix(text, "-")
	if negative {
		return 0, errors.New("value must be non-negative")
	}
	whole, fraction, hasFraction := strings.Cut(text, ".")
	if whole == "" {
		whole = "0"
	}
	for _, digit := range whole {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("value %q is not a plain decimal", text)
		}
	}
	if hasFraction && len(fraction) > 9 {
		return 0, fmt.Errorf("value %q has more than 9 fractional digits", text)
	}
	fraction += strings.Repeat("0", 9-len(fraction))
	for _, digit := range fraction {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("value %q is not a plain decimal", text)
		}
	}
	var wholePart, fractionPart uint64
	if _, err := fmt.Sscanf(whole, "%d", &wholePart); err != nil {
		return 0, fmt.Errorf("value %q whole part is invalid", text)
	}
	if _, err := fmt.Sscanf(fraction, "%d", &fractionPart); err != nil {
		return 0, fmt.Errorf("value %q fraction is invalid", text)
	}
	if wholePart > (^uint64(0)-fractionPart)/1_000_000_000 {
		return 0, fmt.Errorf("value %q overflows the nano range", text)
	}
	return wholePart*1_000_000_000 + fractionPart, nil
}

// CIIOutcome is the computed CII result; NotComputable is the honest
// outcome when no approved config covers the ship type/year.
type CIIOutcome struct {
	NotComputable  bool   `json:"notComputable"`
	AttainedNano   *U64   `json:"attainedCiiNano,omitempty"`
	RequiredNano   *U64   `json:"requiredCiiNano,omitempty"`
	Rating         string `json:"ciiRating,omitempty"`
	ConfigVersion  string `json:"configVersion,omitempty"`
	ConfigCitation string `json:"configCitation,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

// ComputeCII computes attained/required CII and the A-E rating for a ship
// and year. Attained CII = sum(CO2) / (capacity x distance) in
// gCO2/(capacity-tonne x nm), per MEPC.333(76) G2. Any missing configuration
// or capacity/distance datum yields NotComputable — never an estimate.
func (config *CIIConfig) ComputeCII(shipType string, gt, dwt uint64, year int, co2TonnesMilli, distanceNMMilli uint64) CIIOutcome {
	if config == nil {
		return CIIOutcome{NotComputable: true, Reason: "no operator-approved CII configuration is loaded"}
	}
	entry, ok := config.ShipTypes[shipType]
	if !ok {
		return CIIOutcome{NotComputable: true, Reason: "ship type is not covered by the approved CII configuration"}
	}
	zText, ok := entry.ReductionFactorsZNano[fmt.Sprintf("%d", year)]
	if !ok {
		return CIIOutcome{NotComputable: true, Reason: "calendar year is not covered by the approved CII configuration"}
	}
	var capacity uint64
	if entry.CapacityField == "DWT" {
		capacity = dwt
	} else {
		capacity = gt
	}
	if capacity == 0 {
		return CIIOutcome{NotComputable: true, Reason: "capacity datum (" + entry.CapacityField + ") is absent"}
	}
	if distanceNMMilli == 0 {
		return CIIOutcome{NotComputable: true, Reason: "distance travelled is zero"}
	}
	aNano, _ := parseNano(entry.ReferenceLineANano)
	cNano, _ := parseNano(entry.ReferenceLineCNano)
	zNano, _ := parseNano(zText)

	// Attained CII (nano) = co2_grams / (capacity x distance_nm) x 1e9.
	// co2_grams = co2_tonnes_milli x 1e3; distance_nm = distance_milli / 1e3.
	// => attained_nano = co2_milli x 1e15 / (capacity x distance_milli).
	// big.Int: the intermediate exceeds uint64 for plausible inputs.
	numerator := new(big.Int).Mul(new(big.Int).SetUint64(co2TonnesMilli), big.NewInt(1_000_000_000_000_000))
	denominator := new(big.Int).Mul(new(big.Int).SetUint64(capacity), new(big.Int).SetUint64(distanceNMMilli))
	attained := new(big.Int).Quo(numerator, denominator)
	if !attained.IsUint64() {
		return CIIOutcome{NotComputable: true, Reason: "attained CII overflows the fixed-point range"}
	}
	attainedNano := attained.Uint64()

	// Required CII = (1 - z) x a x capacity^(-c) (G2/G3). The exponent is
	// transcendental, so this one evaluation uses float64 internally; the
	// parameters themselves are the exact configured nano integers and the
	// result is immediately rounded back to nano fixed-point.
	a := float64(aNano) / 1e9
	c := float64(cNano) / 1e9
	z := float64(zNano) / 1e9
	required := (1 - z) * a * math.Pow(float64(capacity), -c)
	if required <= 0 || math.IsNaN(required) || math.IsInf(required, 0) {
		return CIIOutcome{NotComputable: true, Reason: "required CII is not computable from the configured parameters"}
	}
	requiredNano := uint64(math.Round(required * 1e9))

	rating := ciiRating(attainedNano, requiredNano, entry)

	attainedU := U64(attainedNano)
	requiredU := U64(requiredNano)
	return CIIOutcome{
		AttainedNano:   &attainedU,
		RequiredNano:   &requiredU,
		Rating:         rating,
		ConfigVersion:  config.Version,
		ConfigCitation: entry.Citation,
	}
}

// ciiRating bands attained against required x the configured boundary
// multipliers (G4), integer-only.
func ciiRating(attainedNano, requiredNano uint64, entry CIIShipTypeCfg) string {
	boundaries := make([]uint64, 4)
	for i, text := range entry.RatingBoundariesNano {
		boundaries[i], _ = parseNano(text)
	}
	ratings := []string{"A", "B", "C", "D"}
	for i, multiplier := range boundaries {
		// boundary = required_nano x multiplier_nano / 1e9 (128-bit-safe via big).
		boundary := new(big.Int).Quo(
			new(big.Int).Mul(new(big.Int).SetUint64(requiredNano), new(big.Int).SetUint64(multiplier)),
			big.NewInt(1_000_000_000))
		if new(big.Int).SetUint64(attainedNano).Cmp(boundary) <= 0 {
			return ratings[i]
		}
	}
	return "E"
}

// CIIConfigSummary is the healthz/config disclosure (never the parameters).
type CIIConfigSummary struct {
	Loaded    bool   `json:"loaded"`
	Version   string `json:"version,omitempty"`
	Citation  string `json:"citation,omitempty"`
	ShipTypes int    `json:"shipTypes,omitempty"`
}

// Summary renders the disclosure form.
func (config *CIIConfig) Summary() CIIConfigSummary {
	if config == nil {
		return CIIConfigSummary{Loaded: false}
	}
	return CIIConfigSummary{Loaded: true, Version: config.Version, Citation: config.Citation, ShipTypes: len(config.ShipTypes)}
}

// Deadlines is the configurable compliance calendar (IMO DCS defaults per
// spec §1.1; Nigerian regulation may differ, so nothing is hardcoded in the
// workflow logic itself).
type Deadlines struct {
	// ReportSubmission is the operator report deadline, "MM-DD" (default 03-31).
	ReportSubmission string
	// SoCIssuance is the Statement of Compliance deadline (default 05-31).
	SoCIssuance string
	// GISISForward is the flag-state forwarding deadline (default 06-30).
	GISISForward string
}

// DeadlinesFromEnv resolves the compliance calendar from
// MRV_DEADLINE_REPORT_MM_DD / MRV_DEADLINE_SOC_MM_DD / MRV_DEADLINE_GISIS_MM_DD
// with the IMO DCS defaults; malformed values fail closed.
func DeadlinesFromEnv() (Deadlines, error) {
	deadlines := Deadlines{ReportSubmission: "03-31", SoCIssuance: "05-31", GISISForward: "06-30"}
	for name, target := range map[string]*string{
		"MRV_DEADLINE_REPORT_MM_DD": &deadlines.ReportSubmission,
		"MRV_DEADLINE_SOC_MM_DD":    &deadlines.SoCIssuance,
		"MRV_DEADLINE_GISIS_MM_DD":  &deadlines.GISISForward,
	} {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			continue
		}
		parsed, err := time.Parse("01-02", raw)
		if err != nil {
			return Deadlines{}, fmt.Errorf("%s must be MM-DD: %w", name, err)
		}
		*target = parsed.Format("01-02")
	}
	return deadlines, nil
}

// PastDeadline reports whether the (month, day) of at falls after the
// "MM-DD" deadline in its calendar year (advisory only).
func PastDeadline(deadline string, at time.Time) bool {
	parsed, err := time.Parse("01-02", deadline)
	if err != nil {
		return false
	}
	boundary := time.Date(at.UTC().Year(), parsed.Month(), parsed.Day()+1, 0, 0, 0, 0, time.UTC)
	return !at.UTC().Before(boundary)
}
