CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name = 'updated_at'
        GROUP BY table_schema, table_name
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP',
            r.table_schema,
            r.table_name
        );

        EXECUTE format(
            'DROP TRIGGER IF EXISTS set_updated_at ON %I.%I',
            r.table_schema,
            r.table_name
        );

        EXECUTE format(
            'CREATE TRIGGER set_updated_at BEFORE UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()',
            r.table_schema,
            r.table_name
        );
    END LOOP;
END $$;
