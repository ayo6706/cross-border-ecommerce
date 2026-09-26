#!/usr/bin/env bash
# Runs every mandatory verification gate (see .agents/rules/verification-evidence.md).
# CI runs this same script (.github/workflows/ci.yml); a gate that exists only in CI or only here
# is a bug.
#
# Integration tests use $TEST_DATABASE_URL and $TEST_REDIS_URL when set; otherwise throwaway
# PostgreSQL 16 and Redis 7 containers are started on ports $VERIFY_PG_PORT (default 55432)
# and $VERIFY_REDIS_PORT (default 56379) and removed on exit.
# Any skipped test counts as a failure: a skip is not a pass.
# VERIFY_RACE=1 adds -race (needs CGO; CI always sets it).
#
# Exit code: 0 when every gate passed, 1 otherwise.
set -uo pipefail

cd "$(dirname "$0")/.."

export GOGC="${GOGC:-50}" # keeps the linker inside small-RAM Windows machines
GO_FLAGS=(-p 1 -count=1)
[[ "${VERIFY_RACE:-}" == "1" ]] && GO_FLAGS+=(-race)

find_cmd() {
	local cmd="$1"
	if command -v "$cmd" >/dev/null 2>&1; then
		echo "$cmd"
	elif command -v "$cmd.exe" >/dev/null 2>&1; then
		echo "$cmd.exe"
	else
		echo ""
	fi
}

GO_BIN=$(find_cmd go)
GOFMT_BIN=$(find_cmd gofmt)
GOLANGCI_BIN=$(find_cmd golangci-lint)
SQLC_BIN=$(find_cmd sqlc)
DOCKER_BIN=$(find_cmd docker)

# The pinned golangci-lint version; CI installs it from the same file (golangci-lint-action version-file).
GOLANGCI_LINT_VERSION=$(tr -d '[:space:]' <.golangci-lint-version 2>/dev/null)
GOLANGCI_LINT_VERSION=${GOLANGCI_LINT_VERSION#v}

declare -a results=()
failed=0

record() { # record <gate> <status> [detail]
	results+=("$(printf '| %-34s | %s |' "$1" "$2${3:+ — $3}")")
	[[ "$2" == "pass" ]] || failed=1
}

run_quiet_gate() { # passes when the command succeeds and prints nothing
	local name=$1
	shift
	echo "==> $name"
	local out
	out=$("$@" 2>&1)
	local code=$?
	if [[ $code -eq 0 && -z "$out" ]]; then
		record "$name" pass
	else
		echo "$out"
		record "$name" FAIL "see output above"
	fi
}

# Fetch modules first: the quiet gates below treat any output, including "go: downloading", as a finding.
if [[ -n "$GO_BIN" ]]; then
	run_quiet_gate "go mod download" "$GO_BIN" mod download
fi

if [[ -n "$GOFMT_BIN" && -n "$GO_BIN" ]]; then
	mapfile -t pkg_dirs < <("$GO_BIN" list -f '{{.Dir}}' ./...) # same scope as go vet/test
	run_quiet_gate "gofmt -l" "$GOFMT_BIN" -l "${pkg_dirs[@]}"
else
	record "gofmt -l" FAIL "gofmt or go not found"
fi

if [[ -n "$GO_BIN" ]]; then
	run_quiet_gate "go vet" "$GO_BIN" vet ./...
	run_quiet_gate "staticcheck" "$GO_BIN" tool staticcheck ./... # version pinned by the tool directive in go.mod
else
	record "go vet" FAIL "go not found"
	record "staticcheck" FAIL "go not found"
fi

lint_hint="go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$GOLANGCI_LINT_VERSION"
lint_version=$([[ -n "$GOLANGCI_BIN" ]] && "$GOLANGCI_BIN" version 2>&1)
if [[ -z "$GOLANGCI_LINT_VERSION" ]]; then
	record "golangci-lint" FAIL ".golangci-lint-version missing or empty"
elif [[ -z "$GOLANGCI_BIN" ]]; then
	record "golangci-lint" FAIL "not installed ($lint_hint)"
elif [[ $lint_version != *"version $GOLANGCI_LINT_VERSION "* && $lint_version != *"version v$GOLANGCI_LINT_VERSION "* ]]; then
	echo "$lint_version"
	record "golangci-lint" FAIL "installed version is not v$GOLANGCI_LINT_VERSION ($lint_hint)"
else
	run_quiet_gate "golangci-lint" "$GOLANGCI_BIN" run ./...
fi

if [[ -n "$SQLC_BIN" ]]; then
	run_quiet_gate "sqlc diff" "$SQLC_BIN" diff
else
	record "sqlc diff" FAIL "not installed (go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0)"
fi

container=""
redis_container=""
cleanup() {
	if [[ -n "$container" && -n "$DOCKER_BIN" ]]; then
		"$DOCKER_BIN" rm -f "$container" >/dev/null 2>&1 || true
	fi
	if [[ -n "$redis_container" && -n "$DOCKER_BIN" ]]; then
		"$DOCKER_BIN" rm -f "$redis_container" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
	if [[ -n "$DOCKER_BIN" ]]; then
		port="${VERIFY_PG_PORT:-55432}"
		container="cb-verify-pg-$$"
		echo "==> starting throwaway PostgreSQL ($container on :$port)"
		if "$DOCKER_BIN" run -d --rm --name "$container" -e POSTGRES_PASSWORD=postgres \
			-e POSTGRES_DB=crossborder_test -p "$port:5432" postgres:16-alpine >/dev/null; then
			for _ in $(seq 1 40); do
				"$DOCKER_BIN" exec "$container" pg_isready -U postgres -d crossborder_test >/dev/null 2>&1 && break
				sleep 1
			done
			sleep 2
			export TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$port/crossborder_test?sslmode=disable"
		else
			container=""
		fi
	fi
fi

if [[ -z "${TEST_REDIS_URL:-}" ]]; then
	if [[ -n "$DOCKER_BIN" ]]; then
		rport="${VERIFY_REDIS_PORT:-56379}"
		redis_container="cb-verify-redis-$$"
		echo "==> starting throwaway Redis ($redis_container on :$rport)"
		if "$DOCKER_BIN" run -d --rm --name "$redis_container" -p "$rport:6379" redis:7-alpine >/dev/null; then
			for _ in $(seq 1 30); do
				"$DOCKER_BIN" exec "$redis_container" redis-cli ping >/dev/null 2>&1 && break
				sleep 1
			done
			sleep 1
			export TEST_REDIS_URL="redis://127.0.0.1:$rport"
		else
			redis_container=""
		fi
	fi
fi

echo "==> go test (unit + integration)"
if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
	record "integration database" FAIL "no TEST_DATABASE_URL and docker unavailable — integration NOT RUN"
fi
if [[ -z "${TEST_REDIS_URL:-}" ]]; then
	record "integration redis" FAIL "no TEST_REDIS_URL and docker unavailable — live Redis suites NOT RUN"
fi

if [[ -z "$GO_BIN" ]]; then
	record "go test ./..." FAIL "go not found"
else
	test_out=$("$GO_BIN" test "${GO_FLAGS[@]}" -v ./... 2>&1)
	test_code=$?
	skips=$(grep -c -- '--- SKIP' <<<"$test_out")
	live=$(grep -cE -- '^--- PASS: Test[A-Za-z0-9_]*_Live' <<<"$test_out")
	if [[ $test_code -ne 0 ]]; then
		grep -E -- '--- FAIL|^FAIL|panic:|_test.go:[0-9]+:' <<<"$test_out" | head -60
		record "go test ./..." FAIL "failing tests (see above)"
	else
		record "go test ./..." pass "${GO_FLAGS[*]}"
	fi
	if [[ $skips -gt 0 ]]; then
		grep -A1 -- '--- SKIP' <<<"$test_out" | head -20
		record "skipped tests" FAIL "$skips skipped (a skip is not a pass)"
	else
		record "skipped tests" pass "0 skipped"
	fi
	record "live test suites ran" "$([[ $live -gt 0 ]] && echo pass || echo FAIL)" "$live suites"
fi

echo
echo "## Verification"
echo "| Gate | Result |"
echo "|---|---|"
printf '%s\n' "${results[@]}"

exit $failed
