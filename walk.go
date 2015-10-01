package main

import (
	"os"
	"path/filepath"
)

type WalkFunc func(path string, info os.FileInfo, err error) error

func walk(path string, info os.FileInfo, walkFn WalkFunc) error {
	err := walkFn(path, info, nil)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return nil
	}

	names, err := readDirNames(path)
	if err != nil {
		return walkFn(path, info, err)
	}

	for _, name := range names {
		filename := filepath.Join(path, name)
		fileInfo, err := os.Lstat(filename)
		if err != nil {
			if err := walkFn(filename, fileInfo, err); err != nil {
				return err
			}
		} else {
			err = walk(filename, fileInfo, walkFn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func Walk(root string, walkFn WalkFunc) error {
	info, err := os.Lstat(root)
	if err != nil {
		return walkFn(root, nil, err)
	}
	return walk(root, info, walkFn)
}

func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return names, nil
}
