
test:
	go test ./...

testv:
	go test -v ./...

export:
	want export-repo glfstar/testdata/
	want export-repo glfszip/testdata/