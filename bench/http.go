package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type HTTP_KVS struct {
	mu       sync.RWMutex
	elements map[string]string
}

type GetResponse struct {
	Value string
}

func (h *HTTP_KVS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	key := r.URL.Query().Get("key")

	if val, ok := h.elements[key]; ok {
		buf, err := json.Marshal(&GetResponse{Value: val})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(buf)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}

	return
}

func bench_http() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	httpKvs := &HTTP_KVS{
		elements: map[string]string{"key": "value"},
	}
	http.Handle("/", httpKvs)
	go http.ListenAndServe(":8080", nil)
	time.Sleep(2 * time.Second)

	wg := &sync.WaitGroup{}
	ch := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(ch chan struct{}) {
			defer wg.Done()
			for _ = range ch {
				//// HTTP I/F
				resp, err := http.Get("http://localhost:8080/?key=key")
				if err != nil {
					log.Fatal(err)
				}
				buf, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Fatal(err)
				}
				getResp := GetResponse{}
				if err := json.Unmarshal(buf, &getResp); err != nil {
					log.Fatal(err)
				}
				resp.Body.Close()
			}
		}(ch)
	}

	start := time.Now()
	for i := 0; i < numJobs; i++ {
		ch <- struct{}{}
	}

	close(ch)

	fmt.Printf("Waiting for workers...\n")
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("%d jobs in %0.2f seconds (%0.2f jobs/sec)\n", numJobs, float64(elapsed)/float64(time.Second), float64(numJobs)/(float64(elapsed)/float64(time.Second)))
}
