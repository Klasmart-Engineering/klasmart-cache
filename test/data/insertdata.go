package main

import (
	"fmt"
	"math/rand"

	"github.com/KL-Engineering/kidsloop-cache/test/entity"
	"github.com/jinzhu/gorm"
	_ "gorm.io/driver/mysql"
)

const (
	connStr  = "root:Badanamu123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	dataSize = 10000
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func generateRecordE() *entity.RecordE {
	return &entity.RecordE{
		ID:      newID(),
		Title:   randString(16),
		Content: randString(64),
	}
}
func generateRecordD() *entity.RecordD {
	return &entity.RecordD{
		ID:      newID(),
		Title:   randString(16),
		Content: randString(64),
	}
}
func generateRecordC() *entity.RecordC {
	return &entity.RecordC{
		ID:       newID(),
		Name:     randString(16),
		RealName: randString(24),
	}
}
func generateRecordB() *entity.RecordB {
	return &entity.RecordB{
		ID:   newID(),
		Name: randString(32),
	}
}
func generateRecordA() *entity.RecordA {
	return &entity.RecordA{
		ID:          newID(),
		Name:        randString(16),
		Keywords:    randString(24),
		Description: randString(64),
	}
}
func linkRecordA(r *entity.RecordA,
	bList []*entity.RecordB,
	cList []*entity.RecordC,
	dList []*entity.RecordD) {

	bIndex := rand.Int() % len(bList)
	r.BID = bList[bIndex].ID

	cIndex := rand.Int() % len(cList)
	r.CID = cList[cIndex].ID

	dIndex := rand.Int() % len(dList)
	r.DID = dList[dIndex].ID

}
func linkRecordB(r *entity.RecordB,
	dList []*entity.RecordD) {
	dIndex := rand.Int() % len(dList)
	r.DID = dList[dIndex].ID
}
func linkRecordD(r *entity.RecordD,
	eList []*entity.RecordE) {
	eIndex := rand.Int() % len(eList)
	r.EID = eList[eIndex].ID
}

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
func newID() string {
	num := rand.Int63()
	prefix := randString(6)
	return fmt.Sprintf("%v%x", prefix, num)
}

func autoMigrate(db *gorm.DB) {
	db.AutoMigrate(entity.RecordA{})
	db.AutoMigrate(entity.RecordB{})
	db.AutoMigrate(entity.RecordC{})
	db.AutoMigrate(entity.RecordD{})
	db.AutoMigrate(entity.RecordE{})
}

func main() {
	db, err := gorm.Open("mysql", connStr)
	if err != nil {
		panic(err)
	}
	autoMigrate(db)

	recordAList := make([]*entity.RecordA, dataSize)
	recordBList := make([]*entity.RecordB, dataSize)
	recordCList := make([]*entity.RecordC, dataSize)
	recordDList := make([]*entity.RecordD, dataSize)
	recordEList := make([]*entity.RecordE, dataSize)
	for i := 0; i < dataSize; i++ {
		recordAList[i] = generateRecordA()
		recordBList[i] = generateRecordB()
		recordCList[i] = generateRecordC()
		recordDList[i] = generateRecordD()
		recordEList[i] = generateRecordE()
	}
	for i := 0; i < dataSize; i++ {
		linkRecordA(recordAList[i], recordBList, recordCList, recordDList)
		linkRecordB(recordBList[i], recordDList)
		linkRecordD(recordDList[i], recordEList)

		err := db.Save(recordAList[i]).Error
		if err != nil {
			fmt.Println("insert A failed, err:", err)
		}
		err = db.Save(recordBList[i]).Error
		if err != nil {
			fmt.Println("insert B failed, err:", err)
		}
		err = db.Save(recordCList[i]).Error
		if err != nil {
			fmt.Println("insert C failed, err:", err)
		}
		err = db.Save(recordDList[i]).Error
		if err != nil {
			fmt.Println("insert D failed, err:", err)
		}
		err = db.Save(recordEList[i]).Error
		if err != nil {
			fmt.Println("insert E failed, err:", err)
		}
	}
}
