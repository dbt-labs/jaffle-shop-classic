
CREATE TABLE models(
    model_name TEXT NOT NULL COLLATE NOCASE,
    cte_name TEXT NOT NULL COLLATE NOCASE,
    cte_type TEXT NOT NULL COLLATE NOCASE,

    CONSTRAINT pk_models PRIMARY KEY (model_name, cte_name)
);

CREATE TABLE tests(
    model_name TEXT NOT NULL COLLATE NOCASE,
    cte_name TEXT COLLATE NOCASE
);


CREATE VIEW coverage_flags AS
    SELECT
        models.model_name,
        models.cte_name,
        (tests.model_name IS NOT NULL) AS coverage_flag
    FROM models
        LEFT JOIN tests
            ON  models.model_name = tests.model_name
            AND models.cte_name = COALESCE(tests.cte_name, 'final') COLLATE NOCASE
    WHERE models.cte_type != 'import'
;

/* Model coverage */
-- SELECT
--     model_name,
--     1.0 * SUM(coverage_flag) / COUNT(*) AS coverage
-- FROM coverage_flags
-- GROUP BY model_name
-- ;

/* Total coverage */
-- SELECT 1.0 * SUM(coverage_flag) / COUNT(*) AS coverage
-- FROM coverage_flags
-- ;
