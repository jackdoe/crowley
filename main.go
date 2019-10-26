package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-metro"
)

var counter = uint64(0)

func crawl(h *http.Client, ua, url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", ua)
	req.Header.Set("Connection", "close")
	req.Close = true

	resp, err := h.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

var ErrExists = errors.New("exists")

func gZipData(data []byte) (compressedData []byte, err error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)

	_, err = gz.Write(data)
	if err != nil {
		return
	}

	if err = gz.Flush(); err != nil {
		return
	}

	if err = gz.Close(); err != nil {
		return
	}

	compressedData = b.Bytes()

	return
}

func downloadAndStore(h *http.Client, ua, root, domain string) (time.Duration, int, error) {
	t0 := time.Now()
	root = path.Join(root, fmt.Sprintf("%v", metro.Hash64Str(domain, 0)%255), fmt.Sprintf("%v", metro.Hash64Str(domain, 1024)%255), fmt.Sprintf("%v", metro.Hash64Str(domain, 2048)%255))
	os.MkdirAll(root, 0700)
	fn := path.Join(root, domain+".gz")
	fnErr := path.Join(root, domain+".err")
	if _, err := os.Stat(fnErr); os.IsNotExist(err) {
		if _, err := os.Stat(fn); os.IsNotExist(err) {
			fnTmp := fn + ".tmp"
			b, err := crawl(h, ua, "http://"+domain)
			if err != nil {
				err = ioutil.WriteFile(fnErr, []byte(err.Error()), 0700)
				if err != nil {
					return time.Since(t0), 0, err
				}
				return time.Since(t0), 0, err
			}
			compressed, err := gZipData(b)
			if err != nil {
				return time.Since(t0), 0, err
			}
			err = ioutil.WriteFile(fnTmp, compressed, 0700)
			if err != nil {
				return time.Since(t0), 0, err
			}
			err = os.Rename(fnTmp, fn)
			if err != nil {
				return time.Since(t0), 0, err
			}

			return time.Since(t0), len(compressed), nil
		} else {
			return time.Since(t0), 0, ErrExists
		}
	} else {
		return time.Since(t0), 0, ErrExists
	}
}

func download(h *http.Client, ua, root, domain string) error {
	c := atomic.AddUint64(&counter, 1)
	dur, size, err := downloadAndStore(h, ua, root, domain)
	if err != nil {
		if err == ErrExists {
			log.Printf("[NOK] %d %v exists", c, domain)
		} else {
			log.Printf("[NOK] %d %v dur: %v err: %v", c, domain, dur, err)
		}
	} else {
		log.Printf("[ OK] %d %v dur: %v, size: %d", c, domain, dur, size)
	}
	return nil
}

func main() {
	root := flag.String("root", "./out", "root output dir")
	ua := flag.String("ua", "crowley bot 1.0", "user agent")
	nWorkers := flag.Int("n-workers", 50, "number of workers")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	jobs := make(chan string)

	close := make(chan bool)

	cleanup := func() {
		log.Printf("closing..")
		for i := 0; i < *nWorkers; i++ {
			close <- true
		}
		log.Printf(".done")
		os.Exit(0)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			cleanup()
		}
	}()

	for i := 0; i < *nWorkers; i++ {
		go func(x int) {

			tr := &http.Transport{
				MaxIdleConns:       1,
				DisableKeepAlives:  true,
				DisableCompression: false,
				Dial: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout: 5 * time.Second,
			}
			h := &http.Client{
				Transport: tr,
				Timeout:   30 * time.Second,
			}

			for {
				select {
				case <-close:
					log.Printf("%d: close received", x)
					return
				case dom := <-jobs:
					download(h, *ua, *root, dom)
					h.CloseIdleConnections()
				}
			}
		}(i)
	}

	for scanner.Scan() {
		dom := scanner.Text()
		jobs <- dom
	}

	if scanner.Err() != nil {
		log.Fatal(scanner.Err())
	}

	cleanup()
}
