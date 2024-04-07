-- Stored procedure to populate "dim date" table
CREATE OR REPLACE FUNCTION populate_dim_date()
            RETURNS VOID AS $$
            DECLARE
                loop_date DATE := '2022-01-01';  -- Start date for populating the "dim date" table
                end_date DATE := CURRENT_DATE;  -- End date for populating the "dim date" table
            BEGIN
                -- Loop through dates from start_date to end_date
                WHILE loop_date <= end_date LOOP
                    -- Insert into "dim date" table
                    INSERT INTO dim_date (full_date, day, month, quarter, year)
                    VALUES (
                        loop_date,
                        EXTRACT(DAY FROM loop_date),
                        EXTRACT(MONTH FROM loop_date),
                        EXTRACT(QUARTER FROM loop_date),
                        EXTRACT(YEAR FROM loop_date)
                        -- Add more column values as needed
                    );
                    -- Move to the next day
                    loop_date := loop_date + INTERVAL '1 day';
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
