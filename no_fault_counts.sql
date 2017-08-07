CREATE OR REPLACE FUNCTION refresh_eviction_counts() 
RETURNS void AS $$
    DECLARE
        ev RECORD;
        omi_count INT DEFAULT 0;
        demo_count INT DEFAULT 0;
        ellis_count INT DEFAULT 0;
        total_count INT DEFAULT 0;
    BEGIN
        DROP TABLE IF EXISTS no_fault_counts;
        CREATE TEMPORARY TABLE no_fault_counts
        ON COMMIT DROP
        AS SELECT
            date_filed,
            type,
            COALESCE( SUM(units), 0 ) AS num_units,
            0 AS omi_count_at_date,
            0 AS ellis_count_at_date,
            0 AS demo_count_at_date,
            0 AS total_count_at_date
        FROM
            no_fault_dec_2014_with_counts
        GROUP BY
            date_filed,
            type
        ORDER BY date_filed ASC;
          
        FOR ev IN SELECT * FROM no_fault_counts LOOP
            CASE ev.type 
                WHEN 'OMI'   THEN omi_count   := omi_count   + ev.num_units;
                WHEN 'DEMO'  THEN demo_count  := demo_count  + ev.num_units;
                WHEN 'ELLIS' THEN ellis_count := ellis_count + ev.num_units;
                ELSE
            END CASE;
            total_count := total_count + ev.num_units;
             
            UPDATE no_fault_counts
            SET
                omi_count_at_date   = omi_count,
                ellis_count_at_date = ellis_count,
                demo_count_at_date  = demo_count,
                total_count_at_date = total_count
            WHERE date_filed = ev.date_filed;
        END LOOP;
        
        
        UPDATE no_fault_dec_2014_with_counts
        SET 
            omi_count_at_date   = no_fault_counts.omi_count_at_date,
            ellis_count_at_date = no_fault_counts.ellis_count_at_date,
            demo_count_at_date  = no_fault_counts.demo_count_at_date,
            total_count_at_date = no_fault_counts.total_count_at_date
        FROM no_fault_counts
        WHERE no_fault_dec_2014_with_counts.date_filed = no_fault_counts.date_filed;
    END;
$$
LANGUAGE 'plpgsql';

SELECT refresh_eviction_counts();
