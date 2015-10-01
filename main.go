package main

import (
	"crypto/md5"
	//"crypto/sha512"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"flag"
	"time"
	"bytes"
)

type File struct {
	path    string
	size    int
	partner string
	sent    bool
}

const (
	q1_workers = 2
	q2_workers = 4
	q3_workers = 1
	q1_c = 1000000
	q2_c = 100000
	q3_c = 10000
	size_max = 1024*1024*100
)

var (
	dry bool
	sanity bool
	verbose bool

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

	stat_start time.Time
	stat_files int64
	stat_files_done int64
	stat_files_saved int64
	stat_bytes int64
	stat_bytes_done int64
	stat_bytes_saved int64
)

func main() {
	flag.BoolVar(&dry, "dry", false, "Dry run")
	flag.BoolVar(&sanity, "sanity", false, "Sanity check (slower)")
	flag.BoolVar(&verbose, "v", false, "Print files as we go")
	flag.Parse()

	if dry {
		fmt.Println("dry running...")
	} else {
		fmt.Println("running...")
	}
	if sanity {
		fmt.Println("sanity checks enabled")
	}

	stat_start = time.Now()

	go func() {
		spaces := ""
		dels := ""
		for i := 0; i < 1024; i++ {
			spaces = spaces + " "
			dels = dels + "\b"
		}
		last := ""
		for {
			time.Sleep(time.Millisecond * 10)
			stats := fmt.Sprintf("   %d/%d %s/%s %d/%s",
				stat_files_done, stat_files,
				fmt_size(stat_bytes_done),
				fmt_size(stat_bytes),
				stat_files_saved,
				fmt_size(stat_bytes_saved),
			)
			if stats == last {
				continue
			}

			fmt.Print(spaces[:len(last)])
			fmt.Print(dels[:len(last)])

			fmt.Print(stats)
			fmt.Print(dels[:len(stats)])

			last = stats
		}
	}()

	q1 = make(chan File, q1_c)
	q1_idx = make(map[int]File)
	q1_dups = make(map[string]struct{})

	q2 = make(chan File, q2_c)
	q2_idx = make(map[string]File)

	q3 = make(chan File, q3_c)
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

	for _, p := range flag.Args() {
		fmt.Println("folder: " + p)

		err := Walk(p, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			s := int(info.Size())

			if s < size_max {
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
					atomic.AddInt64(&stat_files, 1)
				}
				atomic.AddInt64(&stat_files, 1)
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
			defer atomic.AddInt64(&stat_files_done, 1)
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
					atomic.AddInt64(&stat_bytes, int64(first.size))
				}
				q2 <- f
				atomic.AddInt64(&stat_bytes, int64(f.size))
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
			n, err := io.Copy(h, fh)
			if err != nil {
				return err
			}
			if n < int64(f.size) {
				return fmt.Errorf("short read on " + f.path)
			}
			atomic.AddInt64(&stat_bytes_done, int64(n))
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

		if verbose {
			if filepath.Base(f.path) == filepath.Base(f.partner) {
				fmt.Println("==", filepath.Base(f.path))
			} else {
				fmt.Println("!!")
			}
			fmt.Println("\t" + f.path)
			fmt.Println("\t" + f.partner)
		}

		if dry {
			atomic.AddInt64(&stat_files_saved, 1)
			atomic.AddInt64(&stat_bytes_saved, int64(f.size))
			continue
		}

		err := func() error {
			q4_mu.Lock()
			defer q4_mu.Unlock()

			defer atomic.AddInt64(&stat_files_saved, 1)
			defer atomic.AddInt64(&stat_bytes_saved, int64(f.size))

			// sanity check
			if sanity {
				o1, err := exec.Command("md5sum", f.path).Output()
				if err != nil {
					log.Println(err)
					os.Exit(1)
				}
				o2, err := exec.Command("md5sum", f.partner).Output()
				if err != nil {
					log.Println(err)
					os.Exit(1)
				}
				if len(o1) < 32 || len(o2) < 32 || string(o1[:32]) != string(o2[:32]) || len(o1) < 32 {
					log.Println("EEEEEK")
					log.Println(string(o1))
					log.Println(string(o2))
					os.Exit(1)
				}
				if verbose {
					fmt.Println("\t" + string(o1[:32]))
				}
			}

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

func fmt_size(i int64) string {
	v := float64(i)
	unit := "bytes"
	if v > 1024 {
		unit = "k"
		v /= 1024
	}
	if v > 1024 {
		unit = "m"
		v /= 1024
	}
	if v > 1024 {
		unit = "g"
		v /= 1024
	}
	return fmt.Sprintf("%.02f%s", v, unit)
}
