DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'logging') THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE logging');
   END IF;

   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'raw') THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE raw');
   END IF;

   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'serving') THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE serving');
   END IF;
END
$do$;
