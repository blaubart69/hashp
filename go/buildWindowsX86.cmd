setlocal
set GOOS=windows
set GOARCH=386
mkdir .\X86
go build -o .\X86
endlocal