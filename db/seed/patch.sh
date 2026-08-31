# geo: mrv_statements_of_compliance trigger requires a VERIFIED annual report
sed -i "/mrv_annual_reports/ s/'DRAFT'/'VERIFIED'/; /mrv_annual_reports/ s/, NULL, '2026-08-31 08:00:00+00', '2026-08-31 08:00:00+00')/, 'seed.verifier@nimasa.gov.ng', '2026-08-31 08:00:00+00', '2026-08-31 08:00:00+00')/" db/seed/seed.sql
