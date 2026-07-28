CARGO ?= cargo

.PHONY: check fmt clippy test test-tsan coverage doc verify build build-fake run fake-server dev dev-smoke compose-verify clean

check:
	$(CARGO) check --locked --workspace --all-targets

fmt:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --locked --workspace --all-targets -- -D warnings

test:
	$(CARGO) test --locked --workspace

# Rust ThreadSanitizer currently requires nightly Rust and a supported Linux target.
test-tsan:
	RUSTFLAGS="-Zsanitizer=thread" \
	RUSTDOCFLAGS="-Zsanitizer=thread" \
	$(CARGO) +nightly test --locked -Zbuild-std --target x86_64-unknown-linux-gnu --workspace

coverage:
	./scripts/check-coverage.sh

doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked --workspace --no-deps

verify: fmt clippy test doc build

build:
	$(CARGO) build --locked --workspace --all-targets

build-fake:
	$(CARGO) build --locked -p fake-server --bin fake-server

run:
	$(CARGO) run --locked -p service-runtime --bin service-workflow

fake-server:
	$(CARGO) run --locked -p fake-server --bin fake-server

dev: build-fake
	@set -eu; \
	command -v curl >/dev/null 2>&1 || { printf '%s\n' 'make dev requires curl' >&2; exit 1; }; \
	./target/debug/fake-server & \
	FAKE_PID=$$!; \
	cleanup() { kill $$FAKE_PID 2>/dev/null || true; wait $$FAKE_PID 2>/dev/null || true; }; \
	trap cleanup INT TERM EXIT; \
	attempts=0; \
	until curl -fsS http://localhost:8093/health >/dev/null 2>&1; do \
		kill -0 $$FAKE_PID 2>/dev/null || { wait $$FAKE_PID; exit 1; }; \
		attempts=$$((attempts + 1)); \
		if [ $$attempts -ge 100 ]; then printf '%s\n' 'Fake server readiness timed out' >&2; exit 1; fi; \
		sleep 0.1; \
	done; \
	printf '%s\n' 'Admin API: http://localhost:18080' \
	  'Health: curl -s http://localhost:18080/health' \
	  'Send event: curl -s -X POST http://localhost:8093/api/send -H "Content-Type: application/json" -d '\''{"id":"m1","type":"AAA","content":"hello","user_id":"u1"}'\'''; \
	RUNTIME_CONFIG=config/profiles/demo/runtime.yaml \
	RULE_ENGINE_CONFIG=config/profiles/demo/rule-engine.yaml \
	LOG_FORMAT=text \
	$(CARGO) run --locked -p service-runtime --bin service-workflow

dev-smoke:
	./scripts/check-dev.sh

compose-verify:
	./scripts/check-compose.sh

clean:
	$(CARGO) clean
