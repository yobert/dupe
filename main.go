package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

type indexType map[string][]string

var index indexType

func main() {
	index = make(indexType, 1000000)

	log.Println("searching...")

	c_all := 0
	c_pot := 0

	for i, p := range os.Args {
		if i == 0 {
			continue
		}

		filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Println(err)
				return nil
			}
			if info.IsDir() {
				return nil
			}
			c_all++
			key := info.Name() + "." + fmt.Sprintf("%d", info.Size())
			//fmt.Println(path + "\t" + key)
			v := index[key]
			if len(v) > 0 {
				c_pot++
			}
			v = append(v, path)
			index[key] = v
			return nil
		})
	}

	fmt.Printf("%d total files, %d potential duplicates\n", c_all, c_pot)

	done := 0
	last := -1
	total := len(index)

	for k, v := range index {
		done++
		d := int(float32(done) / float32(total))
		if d != last {
			fmt.Printf("%d% done (%d / %d)\n", d, done, total)
			last = d
		}

		if len(v) == 0 {
			log.Fatal("wtf: " + k)
		}
		if len(v) == 1 {
			continue
		}

		args := []string{"-d"}
		args = append(args, v...)

		cmd := exec.Command("duperemove", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stdout
		err := cmd.Run()
		if err != nil {
			log.Println(err)
		}
	}
}
