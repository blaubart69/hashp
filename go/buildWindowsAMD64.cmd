setlocal
set GOOS=windows
set GOARCH=amd64
mkdir .\AMD64
go build -o .\AMD64
endlocal