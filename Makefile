tests:
	@. ./.test.env && go clean -testcache && go test -cover -race ./...

%::
	@true