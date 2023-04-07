package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
)

func main(){
	fmt.Println("Starting server ")
	logDir := "data"
	logStore, err := CreateLogStore(logDir, nil)
	if err != nil {
		log.Fatalf("couldn't create log store: %s", err)
	}
    http.HandleFunc("/health", health())
	http.HandleFunc("/get", get(logStore))
	http.HandleFunc("/set", set(logStore))
	http.HandleFunc("/delete", del(logStore))
	http.ListenAndServe("localhost:9780", nil)
}

func health() func(http.ResponseWriter, *http.Request) {
      return func(w http.ResponseWriter, r *http.Request) {
		  w.WriteHeader(200)
		  w.Write([]byte("good"))
		  return
	  }
}

func get(store *Store) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		 key := r.URL.Query().Get("key")
         if key == ""{
			w.WriteHeader(400)
			w.Write([]byte("missing ?key"))
			return
		 }
		 found, err := store.Get(key, w)
		 if err != nil {
			log.Printf("couldn't get %s: %s", key, err)
			w.WriteHeader(500)
			return
		 }

		 if !found {
			w.WriteHeader(404)
			return
		 }
		 if err != nil {
			log.Printf("couldn't get %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
	}
}

func set(store *Store) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(400)
			w.Write([]byte("missing ?key"))
			return
		}


		var expire int
		_expire := req.URL.Query().Get("expire")
		if _expire == "" {
			expire = math.MaxInt64 // Unix milliseconds
		} else {
			i, err := strconv.Atoi(_expire)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("?expire must be an integer"))
				return
			}
			expire = i
		}

		value, err := io.ReadAll(req.Body)
		if err != nil {
			log.Printf("couldn't set %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
        fmt.Println(value)
		err = store.Set(key, expire, value)
		if err != nil {
			log.Printf("couldn't set %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
	}
}

func del(store *Store) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(400)
			w.Write([]byte("missing ?key"))
			return
		}

		err := store.Set(key, 0, []byte(""))
		if err != nil {
			log.Printf("couldn't delete %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
	}
}
