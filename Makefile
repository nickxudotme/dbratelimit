test: test-compile check-tparse
	set -o pipefail && go test -gcflags=all=-l -cover -short   ./... -json | tparse -all

test-compile:
	go build -o /dev/null .

check-tparse:
	@which tparse > /dev/null 2>&1 || (echo "Installing tparse..." && go install github.com/mfridman/tparse@latest)
