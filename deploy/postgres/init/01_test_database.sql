-- Runs once when the postgres volume is first created (docker-entrypoint-initdb.d).
-- A separate database for `make test-integration`, so the storage tests never
-- race the live relay and consumer that poll the application database.
CREATE DATABASE parcellab_test OWNER parcellab;
