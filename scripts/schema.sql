CREATE TABLE IF NOT EXISTS cdc_weekly_deaths (
    id                              SERIAL PRIMARY KEY,
    year                            INTEGER       NOT NULL,
    week                            INTEGER       NOT NULL,
    week_ending_date                DATE,
    jurisdiction                    TEXT          NOT NULL,
    all_cause_deaths                INTEGER,
    natural_cause_deaths            INTEGER,
    heart_disease_deaths            INTEGER,
    covid_19_multiple_cause_deaths  INTEGER,
    covid_19_underlying_cause_deaths INTEGER,
    ingested_at                     TIMESTAMPTZ   DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (year, week, jurisdiction)
);
