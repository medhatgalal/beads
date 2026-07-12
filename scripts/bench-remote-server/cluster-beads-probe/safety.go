package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	probePasswordRoot = "/private/tmp/beads-perf-lab-runtime"
	probeEvidenceRoot = "/private/tmp/beads-fleet-scale-20260712"
	maxPasswordBytes  = 4096
)

func validateProbeEndpoint(host string, port int, database string) error {
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() || host != probeHost {
		return fmt.Errorf("probe host must be fixed numeric loopback")
	}
	if port == 3306 || port == 3307 {
		return fmt.Errorf("production SQL ports 3306 and 3307 are forbidden")
	}
	if port != probePort || database != probeDatabase {
		return fmt.Errorf("probe endpoint is outside the isolated cluster lab")
	}
	return nil
}

func canonicalApprovedPath(path, root string, allowMissingFinal bool) (string, error) {
	if path == "" || root == "" || !filepath.IsAbs(path) || !filepath.IsAbs(root) {
		return "", fmt.Errorf("path and approved root must be absolute")
	}
	for _, component := range strings.Split(filepath.ToSlash(path), "/") {
		if component == ".." {
			return "", fmt.Errorf("parent traversal is forbidden")
		}
	}
	cleanPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	cleanRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil || rel == "." || rel == "" || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path is outside approved root")
	}
	if err := rejectSymlinkComponents(cleanPath, allowMissingFinal); err != nil {
		return "", err
	}
	return cleanPath, nil
}

func rejectSymlinkComponents(path string, allowMissingFinal bool) error {
	volume := filepath.VolumeName(path)
	remainder := strings.TrimPrefix(path, volume)
	components := strings.Split(strings.TrimPrefix(remainder, string(filepath.Separator)), string(filepath.Separator))
	current := volume + string(filepath.Separator)
	for index, component := range components {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) && allowMissingFinal && index == len(components)-1 {
				return nil
			}
			return fmt.Errorf("inspect path component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink path components are forbidden")
		}
		if index < len(components)-1 && !info.IsDir() {
			return fmt.Errorf("non-directory path component")
		}
	}
	return nil
}

func readPasswordFile(path, root string) ([]byte, error) {
	clean, err := canonicalApprovedPath(path, root, false)
	if err != nil {
		return nil, fmt.Errorf("password path rejected: %w", err)
	}
	fd, err := unix.Open(clean, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open password without following links: %w", err)
	}
	file := os.NewFile(uintptr(fd), clean)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("wrap password descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened password: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("password must be a regular file with exact mode 0600")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxPasswordBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded password: %w", err)
	}
	defer clear(data)
	if len(data) > maxPasswordBytes {
		return nil, fmt.Errorf("password exceeds %d-byte bound", maxPasswordBytes)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("password is empty")
	}
	return bytes.Clone(trimmed), nil
}

func writeEvidenceFile(path string, body []byte) error {
	return writeEvidenceFileUnderRoot(path, probeEvidenceRoot, body)
}

func writeEvidenceFileUnderRoot(path, root string, body []byte) error {
	clean, err := canonicalApprovedPath(path, root, true)
	if err != nil {
		return err
	}
	fd, err := unix.Open(clean, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), clean)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("wrap evidence descriptor")
	}
	defer file.Close()
	if _, err := file.Write(body); err != nil {
		return err
	}
	return file.Sync()
}
