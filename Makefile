.PHONY: test
test:
	@go test -coverpkg=./... -coverprofile=coverage.out ./...

.PHONY: clean
clean:
	@rm -rf coverage.cov

.PHONY: coverage
coverage: clean test coverage
	go tool cover -func=coverage.out

.PHONY: coverage-html
coverage-html: clean test coverage
	@go tool cover -html=coverage.out