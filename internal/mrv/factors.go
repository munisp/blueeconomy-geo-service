// Emission-factor resolution and the fixed-point CO2 arithmetic. Factors
// resolve ONLY from source-cited mrv_emission_factors rows (MEPC.245(66) as
// amended by MEPC.308(73)/MEPC.364(79); EU MRV Annex I; CH4/N2O/WtW only
// from MEPC.391(81) and the IMO Fourth GHG Study). There is no default, no
// fallback and no "assumed 3.114": an unresolved factor fails closed with
// ErrFactorUnavailable and no estimate is produced.
package mrv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Factor is one source-cited emission-factor registry row.
type Factor struct {
	FactorKey      string `json:"factorKey"`
	Gas            string `json:"gas"`
	FactorNano     U64    `json:"factorNano"`
	Unit           string `json:"unit"`
	SourceCitation string `json:"sourceCitation"`
	ValidFrom      string `json:"validFrom"` // ISO date
}

// FactorRow is the raw registry row used for factor-set digests.
type FactorRow struct {
	FactorKey      string
	Gas            string
	FactorNano     uint64
	Unit           string
	SourceCitation string
	ValidFrom      time.Time
}

// ResolveFactor returns the CO2 factor row for fuelGrade valid at
// periodDate (latest valid_from <= periodDate). It fails closed with
// ErrFactorUnavailable when no source-cited row resolves.
func ResolveFactor(ctx context.Context, tx pgx.Tx, fuelGrade, gas string, periodDate time.Time) (FactorRow, error) {
	var row FactorRow
	var validFrom time.Time
	err := tx.QueryRow(ctx, `SELECT factor_key, gas, factor_nano, unit, source_citation, valid_from
		FROM mrv_emission_factors
		WHERE factor_key = $1 AND gas = $2 AND valid_from <= $3::date
		ORDER BY valid_from DESC LIMIT 1`, fuelGrade, gas, periodDate.UTC().Format("2006-01-02")).
		Scan(&row.FactorKey, &row.Gas, &row.FactorNano, &row.Unit, &row.SourceCitation, &validFrom)
	if errors.Is(err, pgx.ErrNoRows) {
		return FactorRow{}, fmt.Errorf("%w: %s/%s", ErrFactorUnavailable, fuelGrade, gas)
	}
	if err != nil {
		return FactorRow{}, fmt.Errorf("resolve emission factor %s/%s: %w", fuelGrade, gas, err)
	}
	row.ValidFrom = validFrom
	return row, nil
}

// CO2MilliTonnes computes fuel x factor in milli-tonnes of CO2 with
// integer-only fixed-point arithmetic and explicit overflow guards:
//
//	co2_tonnes_milli = fuel_tonnes_milli (x1e3) * factor_nano (x1e9) / 1e9
//
// The product bound is guarded: fuel_milli <= (2^64-1)/factor_nano.
func CO2MilliTonnes(fuelTonnesMilli, factorNano uint64) (uint64, error) {
	if factorNano == 0 {
		return 0, errors.New("emission factor is zero")
	}
	if fuelTonnesMilli > (^uint64(0))/factorNano {
		return 0, errors.New("fuel x factor overflows the fixed-point range")
	}
	return fuelTonnesMilli * factorNano / 1_000_000_000, nil
}

// FactorSetHash digests the exact factor rows used in a computation:
// sha256 over the newline-joined canonical rows
// "factor_key|gas|factor_nano|unit|source_citation|valid_from(ISO date)"
// sorted by (factor_key, gas, valid_from). Rendered "sha256:<hex>".
func FactorSetHash(rows []FactorRow) string {
	canonical := make([]string, 0, len(rows))
	for _, row := range rows {
		canonical = append(canonical, fmt.Sprintf("%s|%s|%d|%s|%s|%s",
			row.FactorKey, row.Gas, row.FactorNano, row.Unit, row.SourceCitation,
			row.ValidFrom.UTC().Format("2006-01-02")))
	}
	sort.Strings(canonical)
	sum := sha256.New()
	for _, line := range canonical {
		sum.Write([]byte(line))
		sum.Write([]byte("\n"))
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil))
}

// ListFactors returns the full public factor table with citations.
func ListFactors(ctx context.Context, tx pgx.Tx) ([]Factor, error) {
	rows, err := tx.Query(ctx, `SELECT factor_key, gas, factor_nano, unit, source_citation, valid_from
		FROM mrv_emission_factors ORDER BY factor_key, gas, valid_from`)
	if err != nil {
		return nil, fmt.Errorf("list emission factors: %w", err)
	}
	defer rows.Close()
	factors := make([]Factor, 0)
	for rows.Next() {
		var factor Factor
		var nano uint64
		var validFrom time.Time
		if err := rows.Scan(&factor.FactorKey, &factor.Gas, &nano, &factor.Unit, &factor.SourceCitation, &validFrom); err != nil {
			return nil, err
		}
		factor.FactorNano = U64(nano)
		factor.ValidFrom = validFrom.UTC().Format("2006-01-02")
		factors = append(factors, factor)
	}
	return factors, rows.Err()
}
