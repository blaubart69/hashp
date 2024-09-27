Calculate SHA256 hashes of files in a directory tree in parallel.

Wrote this little tool when we had to archive lots of files (123Mio/58TB).
The files resides on a NAS device and we want to read the bytes in parallel to saturate the network.

There are 2 output files:
1, hashes.txt - CSV - "sha256","filesize","filename"
2, errors.txt - the errors occoured when processing
