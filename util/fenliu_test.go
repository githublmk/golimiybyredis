package util

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"testing"
)

var s *Service

func Init() {
	db := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	})
	s1, err2 := NewService("prefix", "suffix", "setkey", "hashkey", db)
	if err2 != nil {
		fmt.Println(err2)
		return
	}
	s = s1
	println(s)
	println(s == nil)
	println("111")
	err := s.AddResource("test1", 120, 1, 2, 50, 0, 5, 120, 0.3)
	err = s.AddResource("test2", 120, 2, 0.2, 50, 0, 5, 120, 0.3)
	err = s.AddResource("test3", 120, 3, 3, 50, 0, 5, 120, 0.3)

	fmt.Println("======err======")
	fmt.Println(err)
	if err != nil {
		log.Fatal(err)
	}
}
func TestName2(t *testing.T) {
	Init()
	take, i, b, err := s.Take("test1")
	fmt.Println("take:", take, " limittype:", i, " isPsss", b, " err:", err)
	take2, i2, b2, err2 := s.Take("test2")
	fmt.Println("take2:", take2, " limittype2:", i2, " isPsss2", b2, " err2:", err2)
	take3, i3, b3, err3 := s.Take("test3")
	fmt.Println("take3:", take3, " limittype3:", i3, " isPsss3", b3, " err3:", err3)

}
