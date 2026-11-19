package main

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDurhamCredentialPipeRejectsRegularFilesAndReadEndpoints(t *testing.T) {
	for _, fd := range []int{-1, 0, 1, 2} {
		if _, err := durhamCredentialPipe(fd); err == nil {
			t.Fatalf("accepted standard/invalid descriptor %d", fd)
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "forbidden")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fileFD, err := unix.Dup(int(file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durhamCredentialPipe(fileFD); err == nil {
		t.Fatal("accepted regular credential file")
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	if _, err := durhamCredentialPipe(int(reader.Fd())); err == nil {
		t.Fatal("accepted credential pipe read end")
	}
	fd, err := unix.Dup(int(writer.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	sink, err := durhamCredentialPipe(fd)
	if err != nil {
		t.Fatal(err)
	}
	sink.Close()
}
