package fence

// Zone category semantics for the WP-10 geofence engine (Phase 19 MPA/EEZ
// protection zones). The category rides on every fence version and is
// snapshotted onto the transition events the version produces, so MPA/EEZ
// incursion alerting never depends on mutable zone metadata.

// ZoneCategory classifies a governed zone.
type ZoneCategory string

const (
	CategoryGeneral           ZoneCategory = "general"
	CategoryMPA               ZoneCategory = "mpa"
	CategoryEEZRestricted     ZoneCategory = "eez_restricted"
	CategoryTrafficSeparation ZoneCategory = "traffic_separation"
	CategoryFishingClosure    ZoneCategory = "fishing_closure"
	CategoryAnchorage         ZoneCategory = "anchorage"
)

// AdmittedZoneCategory reports whether the category is contract-admitted.
// An empty category is normalized to "general" by NormalizeZoneCategory.
func AdmittedZoneCategory(category string) bool {
	switch ZoneCategory(category) {
	case CategoryGeneral, CategoryMPA, CategoryEEZRestricted, CategoryTrafficSeparation, CategoryFishingClosure, CategoryAnchorage:
		return true
	default:
		return false
	}
}

// NormalizeZoneCategory maps the empty category (pre-0019 rows, unset
// request field) to "general" and rejects unknown values fail-closed.
func NormalizeZoneCategory(category string) (ZoneCategory, bool) {
	if category == "" {
		return CategoryGeneral, true
	}
	if AdmittedZoneCategory(category) {
		return ZoneCategory(category), true
	}
	return "", false
}

// protectedCategories are the zone categories whose ENTRY constitutes a
// protection alert (MPA incursion, EEZ-restricted incursion, fishing-closure
// violation). Traffic-separation schemes alert on entry too (lane
// discipline), anchorage and general zones stay informational.
func protectedCategory(category ZoneCategory) bool {
	switch category {
	case CategoryMPA, CategoryEEZRestricted, CategoryTrafficSeparation, CategoryFishingClosure:
		return true
	default:
		return false
	}
}

// ZoneAlert is the deterministic alert-decision rule: a transition into or
// out of a protected-category zone produces the alert token consumers
// (MDA/IUU pipelines) route on; every other transition produces "" (no
// alert, event still recorded). Pure and total.
func ZoneAlert(category ZoneCategory, eventType EventType) string {
	if !protectedCategory(category) {
		return ""
	}
	switch eventType {
	case EventEnter:
		return "PROTECTED_ZONE_ENTRY"
	case EventExit:
		return "PROTECTED_ZONE_EXIT"
	default:
		return ""
	}
}
