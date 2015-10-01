package main

import (
	"crypto/md5"
	"crypto/sha512"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type File struct {
	path    string
	size    int
	partner string
	sent    bool
}

const (
	q1_workers = 1
	q2_workers = 1
	q3_workers = 1
)

var (
	wg1 sync.WaitGroup
	wg2 sync.WaitGroup
	wg3 sync.WaitGroup

	q1      chan File
	q1_idx  map[int]File
	q1_dups map[string]struct{}

	q2     chan File
	q2_mu  sync.Mutex
	q2_idx map[string]File

	q3     chan File
	q3_mu  sync.Mutex
	q3_idx map[string]File

	q4_mu sync.Mutex
)

func main() {

	q1 = make(chan File)
	q1_idx = make(map[int]File)
	q1_dups = make(map[string]struct{})

	q2 = make(chan File)
	q2_idx = make(map[string]File)

	q3 = make(chan File)
	q3_idx = make(map[string]File)

	for i := 0; i < q1_workers; i++ {
		wg1.Add(1)
		go q1_worker()
	}

	for i := 0; i < q2_workers; i++ {
		wg2.Add(1)
		go q2_worker()
	}

	for i := 0; i < q3_workers; i++ {
		wg3.Add(1)
		go q3_worker()
	}

	for _, p := range os.Args[1:] {
		err := Walk(p, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			s := int(info.Size())

			if s < 1024*1024 {
				return nil
			}
			if info.Mode()&os.ModeSymlink > 0 {
				return nil
			}

			path, err = filepath.Abs(filepath.Clean(path))
			if err != nil {
				return err
			}

			// skip exact path duplicates (in case you pass two folders as arguments, and one contains the other, etc)
			_, pathdup := q1_dups[path]
			if pathdup {
				log.Println("skipping duplicated path entry: " + path)
				return nil
			}
			q1_dups[path] = struct{}{}

			f := File{
				path: path,
				size: s,
			}

			first, ok := q1_idx[s]

			if ok {
				if !first.sent {
					q1 <- first
					first.sent = true
					q1_idx[s] = first
				}
				q1 <- f
			} else {
				q1_idx[s] = f
			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	close(q1)
	wg1.Wait()

	//fmt.Println("q1 done")

	close(q2)
	wg2.Wait()

	//fmt.Println("q2 done")

	close(q3)
	wg3.Wait()

	//fmt.Println("q3 done")
}

func q1_worker() {
	defer wg1.Done()

	buf := make([]byte, 4096)

	for {
		f, ok := <-q1
		if !ok {
			return
		}

		//fmt.Println("q1", f.path)
		f.sent = false

		err := func() error {
			h := md5.New()
			fh, err := os.Open(f.path)
			if err != nil {
				return err
			}
			defer fh.Close()
			n, err := fh.Read(buf)
			if err != nil {
				return err
			}
			if n < len(buf) {
				return fmt.Errorf("short read on " + f.path)
			}
			h.Write(buf)
			s := fmt.Sprintf("%x", h.Sum(nil))

			q2_mu.Lock()
			defer q2_mu.Unlock()

			first, ok := q2_idx[s]

			if ok {
				if !first.sent {
					q2 <- first
					first.sent = true
					q2_idx[s] = first
				}
				q2 <- f
			} else {
				q2_idx[s] = f
			}
			return nil
		}()

		if err != nil {
			log.Println(err)
		}
	}
}

func q2_worker() {
	defer wg2.Done()

	buf := make([]byte, 4096)

	for {
		f, ok := <-q2
		if !ok {
			return
		}

		//fmt.Println("q2", f.path)
		f.sent = false

		err := func() error {
			h := sha512.New()
			fh, err := os.Open(f.path)
			if err != nil {
				return err
			}
			defer fh.Close()
			n, err := fh.Read(buf)
			if err != nil {
				return err
			}
			if n < len(buf) {
				return fmt.Errorf("short read on " + f.path)
			}
			h.Write(buf)
			s := fmt.Sprintf("%x", h.Sum(nil))

			q3_mu.Lock()
			defer q3_mu.Unlock()

			first, ok := q3_idx[s]

			if ok {
				f.partner = first.path
				q3 <- f
			} else {
				q3_idx[s] = f
			}
			return nil
		}()

		if err != nil {
			log.Println(err)
		}
	}
}

func q3_worker() {
	defer wg3.Done()

	for {
		f, ok := <-q3
		if !ok {
			return
		}

		fmt.Println("==", f.path, f.partner, "...")

		err := func() error {
			tmp := f.path + "_DERP"
			cmd := exec.Command("cp", "-n", "-p", "--reflink=always", f.partner, tmp)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				return err
			}

			err = os.Rename(tmp, f.path)
			if err != nil {
				return err
			}

			fmt.Println("==", f.path, f.partner, "complete")
			return nil
		}()
		if err != nil {
			log.Println(err)
		}
	}
}
