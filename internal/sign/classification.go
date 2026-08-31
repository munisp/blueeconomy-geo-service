// Package sign builds and signs geo event envelopes. Classification labels
// follow the geo.*.v1 contract ladder; handling is fail-closed: an absent or
// unknown label is rejected, never defaulted.
package sign

import (
	"errors"
	"fmt"
	"strings"
)

// Classification is the handling classification label asserted for geospatial
// content and for the envelope carrying it. The rank orders the labels so
// clearance checks are a single comparison (identical doctrine to
// maritime-intelligence).
type Classification string

const (
	ClassificationPublic       Classification = "PUBLIC"
	ClassificationInternal     Classification = "INTERNAL"
	ClassificationRestricted   Classification = "RESTRICTED"
	ClassificationConfidential Classification = "CONFIDENTIAL"
	ClassificationSecret       Classification = "SECRET"
)

// ErrInvalidClassification is returned when a label is absent or unknown.
var ErrInvalidClassification = errors.New("classification must be one of PUBLIC, INTERNAL, RESTRICTED, CONFIDENTIAL, SECRET")

// ParseClassification validates a raw label fail-closed.
func ParseClassification(raw string) (Classification, error) {
	switch Classification(strings.TrimSpace(raw)) {
	case ClassificationPublic, ClassificationInternal, ClassificationRestricted, ClassificationConfidential, ClassificationSecret:
		return Classification(strings.TrimSpace(raw)), nil
	default:
		return "", ErrInvalidClassification
	}
}

// MustClassification validates an internal label; it panics only on a
// programming error (a constant outside the approved set).
func MustClassification(value Classification) Classification {
	if _, err := ParseClassification(string(value)); err != nil {
		panic(fmt.Sprintf("sign: invalid classification constant %q", value))
	}
	return value
}

// Rank returns the ordinal sensitivity of the label (higher is more
// sensitive). Unknown labels rank as Secret so comparisons fail closed.
func (label Classification) Rank() int {
	switch label {
	case ClassificationPublic:
		return 0
	case ClassificationInternal:
		return 1
	case ClassificationRestricted:
		return 2
	case ClassificationConfidential:
		return 3
	default:
		return 4
	}
}

// Covers reports whether a principal holding this label as clearance may read
// material labelled `event`. Fail-closed: an invalid clearance covers nothing.
func (label Classification) Covers(event Classification) bool {
	if _, err := ParseClassification(string(label)); err != nil {
		return false
	}
	if _, err := ParseClassification(string(event)); err != nil {
		return false
	}
	return label.Rank() >= event.Rank()
}

// MaxClassification returns the more sensitive of two valid labels.
func MaxClassification(a, b Classification) Classification {
	if a.Rank() >= b.Rank() {
		return a
	}
	return b
}
