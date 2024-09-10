setlocal
set GOOS=linux
set GOARCH=amd64
mkdir .\AMD64\linux
go build -o .\AMD64\linux
endlocal