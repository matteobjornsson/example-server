db-start:
    docker run --name postgres-dev --rm -e POSTGRES_PASSWORD=password -e POSTGRES_DB=example -p 5432:5432 -d postgres:15

db-stop:
    docker stop postgres-dev

db-seed:
    docker exec postgres-dev psql -U postgres -d example -c "INSERT INTO scoped_tokens (secret, allowed_paths, limiter_rate_per_second, note) VALUES ('testtoken', ARRAY['/app'], 10, 'test') ON CONFLICT DO NOTHING;"

run:
    POSTGRES_DSN="postgres://postgres:password@localhost:5432/example?sslmode=disable" air
