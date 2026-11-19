package main

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
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
	defer unix.Close(fileFD)
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

func TestDurhamCredentialPipeRejectsTruncatedStandardDescriptors(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires a wider Go int than the OS descriptor")
	}
	if os.Getenv("STOARAMA_TEST_DURHAM_FD_CHILD") == "1" {
		for _, std := range []int{1, 2} {
			alias := int((int64(1) << 32) | int64(std))
			if _, err := durhamCredentialPipe(alias); err == nil {
				t.Fatalf("accepted descriptor alias %d of piped standard descriptor %d", alias, std)
			}
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurhamCredentialPipeRejectsTruncatedStandardDescriptors$")
	cmd.Env = append(os.Environ(), "STOARAMA_TEST_DURHAM_FD_CHILD=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
}

func TestDurhamCredentialPipeRejectsDuplicatedStandardPipes(t *testing.T) {
	if os.Getenv("STOARAMA_TEST_DURHAM_DUP_CHILD") == "1" {
		for _, std := range []int{1, 2} {
			duplicate, err := unix.Dup(std)
			if err != nil {
				t.Fatal(err)
			}
			_, err = durhamCredentialPipe(duplicate)
			unix.Close(duplicate)
			if err == nil {
				t.Fatalf("accepted a duplicate of standard descriptor %d", std)
			}
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurhamCredentialPipeRejectsDuplicatedStandardPipes$")
	cmd.Env = append(os.Environ(), "STOARAMA_TEST_DURHAM_DUP_CHILD=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatal(err, stdout.String(), stderr.String())
	}
}
