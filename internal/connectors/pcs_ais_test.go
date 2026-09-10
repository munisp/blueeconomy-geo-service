package connectors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPCSCoordinateConversion(t *testing.T) {
	// Double degrees → fixed-point micro-degrees at the import boundary.
	require.Equal(t, int32(-4_062_000), microsFromDegrees(-4.062))
	require.Equal(t, int32(39_672_000), microsFromDegrees(39.672))
	require.Equal(t, int32(0), microsFromDegrees(0))
	require.Equal(t, uint32(10_500), milliknotsFromKnots(10.5))
	require.Equal(t, uint32(359_900), millidegreesFromDegrees(359.9))
}

func TestPCSImporterValidate(t *testing.T) {
	importer := &PCSImporter{}
	require.Error(t, importer.Validate())
	importer.DSN = "postgres://pcs"
	require.Error(t, importer.Validate()) // no pipeline
}
