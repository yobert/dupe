package main

import (
	"fmt"
	"io"
	"log"
	"os"
	//	"os/exec"
	"crypto/md5"
	"crypto/sha256"
	"github.com/cheggaaa/pb"
	"path/filepath"
)

type keyType struct {
	//Base string
	Size int
}
type indexType map[keyType][]string

var index indexType

func Bar(i int) *pb.ProgressBar {
	b := pb.New(i)
	b.Format("[=>-]")
	b.Start()
	return b
}

func main() {
	index = make(indexType, 1000000)

	if len(os.Args) == 1 {
		fmt.Println("pass me file paths to search")
		os.Exit(1)
		return
	}

	log.Println("searching...")

	c_all := 0
	c_pot := 0

	b := Bar(len(os.Args) - 1)

	for i, p := range os.Args {
		if i == 0 {
			continue
		}
		b.Increment()

		filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Println(err)
				return nil
			}
			if info.IsDir() {
				return nil
			}
			if info.Size() < 1024*1024 {
				return nil
			}
			if info.Mode()&os.ModeSymlink > 0 {
				return nil
			}
			c_all++
			key := keyType{
				//Base: info.Name(),
				Size: int(info.Size()),
			}
			v := index[key]
			if len(v) > 0 {
				c_pot++
			}
			v = append(v, path)
			index[key] = v
			return nil
		})
	}
	b.Finish()

	real_count := 0
	real_total := 0
	real_bytes := 0
	for k, v := range index {
		if len(v) > 1 {
			real_count++
			real_total += len(v)
			real_bytes += k.Size
		}
	}

	if real_count == 0 {
		fmt.Println("nothin to dedupe.")
		os.Exit(1)
		return
	}

	b = Bar(real_count)

	buf := make([]byte, 4096)

	total_savings := 0

	for k, v := range index {
		siz := k.Size

		if len(v) == 0 {
			log.Fatal("wtf")
		}
		if len(v) == 1 {
			continue
		}
		b.Increment()

		pre := make(map[string][]string, len(v))

		for _, f := range v {
			h := md5.New()
			fh, err := os.Open(f)
			if err != nil {
				log.Println(err)
				continue
			}
			n, err := fh.Read(buf)
			fh.Close()
			if n < len(buf) {
				log.Println("short read on " + f)
				continue
			}
			if err != nil {
				log.Println(err)
				continue
			}
			h.Write(buf)
			s := fmt.Sprintf("%x", h.Sum(nil))
			pre[s] = append(pre[s], f)
		}

		c := 0
		for _, v := range pre {
			if len(v) > 1 {
				c++
			}
		}
		if c == 0 {
			continue
		}

		final := make(map[string][]string, c)

		for _, v := range pre {
			if len(v) == 0 {
				log.Fatal("wtf")
			}
			if len(v) == 1 {
				continue
			}

			for _, f := range v {
				h := sha256.New()
				fh, err := os.Open(f)
				if err != nil {
					log.Println(err)
					continue
				}
				if _, err := io.Copy(h, fh); err != nil {
					log.Println(err)
					fh.Close()
					continue
				}
				fh.Close()

				s := fmt.Sprintf("%x", h.Sum(nil))
				final[s] = append(final[s], f)
			}
		}

		for _, v := range final {
			if len(v) == 0 {
				log.Fatal("wtf")
			}
			if len(v) == 1 {
				continue
			}
			//			fmt.Printf("%s\n", k)
			for _, _ = range v {
				//				fmt.Printf("\t%s\n", f)

				total_savings += siz
			}
		}

		/*		//args := []string{"-ahl"}
				args := []string{"-d"}
				args = append(args, v...)

				//cmd := exec.Command("ls", args...)
				cmd := exec.Command("duperemove", args...)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stdout
				err := cmd.Run()
				if err != nil {
					log.Println(err)
				}*/
	}
	b.Finish()

	fmt.Println("total savings:", fmt_size(total_savings))
}

func fmt_size(i int) string {
	v := float64(i)
	unit := "bytes"
	if v > 1024 {
		unit = "KB"
		v /= 1024
	}
	if v > 1024 {
		unit = "MB"
		v /= 1024
	}
	if v > 1024 {
		unit = "GB"
		v /= 1024
	}
	return fmt.Sprintf("%.02f %s", v, unit)
}
