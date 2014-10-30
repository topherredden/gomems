//view-source:http://steamcommunity.com/profiles/76561197960265729/games/?tab=all

package main

import "fmt"
import "io/ioutil"
import "net/http"
import "regexp"
import "encoding/json"
import "github.com/mxk/go-sqlite/sqlite3"
import "time"

type ColumnData struct {
    Value string `json:"val"`
}

type ColumnRootData struct {
    Column1 ColumnData `json:"1"`
    Column2 ColumnData `json:"2"`
    Column3 ColumnData `json:"3"`
}

type ThingData struct {
    Columns    ColumnRootData `json:"columns"`
}

type MemData struct {
    MemId            int `json:"id"`
    Rating           int `json:"rating"`
    Thing            ThingData `json:"thing"`
}

const baseMemId int64 = 5001

var dbConn *sqlite3.Conn
var memChan chan int64
var dbChan chan string
var currentId int64 = baseMemId

func main() {

    loadDatabase()
    defer dbConn.Close()

    memChan = make(chan int64, 1)
    dbChan = make(chan string, 1)
    defer close(dbChan)

    go memProducer(memChan)
    go dbExec(dbChan)

    for i := 0; i<1; i++ {
        go scrapeMem(memChan, dbChan)
    }

    for true {
        time.Sleep(1 * 1e9)
    }
}

func loadDatabase() {
    dbConn, _ = sqlite3.Open("appdata.db")

    dbConn.Exec("CREATE TABLE mems(id BIGINT PRIMARY KEY, updated DATE, valid BOOLEAN, connected BOOLEAN, ratings BIGINT, value STRING);")
    dbConn.Exec("CREATE TABLE global(current_id BIGINT);")

    /*rows, _ := dbConn.Query("SELECT current_id FROM global")
    defer rows.Close()
    rows.Next()
    rows.Scan(&currentId)*/

    if currentId == baseMemId {
        currentIdInsert := fmt.Sprintf(`INSERT OR REPLACE INTO global(current_id) VALUES(%v)`, baseMemId)
        dbConn.Exec(currentIdInsert)
    }

    fmt.Println(currentId)
}

func scrapeMem(c chan int64, dbChan chan string) {
    var v int64
    ok := true

    for ok {
        if v, ok = <-c; ok {
            var data MemData

            var mem_id = v
            var mem_address = fmt.Sprintf("http://www.memrise.com/mem/%v/", mem_id)
            var jsonMatch = regexp.MustCompile(`MEMRISE\.mem_view\.init\((.*)\);`)

            var valid = 0
            var connected = 1

            resp, err := http.Get(mem_address)
            if err != nil {
                fmt.Println("HTTP GET Error: ", err)
                connected = 0
            }

            if connected == 1 {
                defer resp.Body.Close()

                body, err := ioutil.ReadAll(resp.Body)
                if err != nil {
                    fmt.Println("Body Read Error: ", err)
                }

                res := jsonMatch.FindStringSubmatch(string(body))

                if len(res) > 0 && len(res[1]) > 0 {
                    valid = 1

                    //fmt.Println((res[1]))
                    err = json.Unmarshal([]byte(res[1]), &data)
                    if err != nil {
                        fmt.Println("JSON Error: ", err, mem_id)
                        valid = 0
                    }
                }
            }

            t := time.Now()
            fmt.Println(data.Thing.Columns.Column1.Value)
            memInsert := fmt.Sprintf(`INSERT OR REPLACE INTO mems(id, updated, valid, connected, ratings, value) VALUES(%v, %v, %d, %d, %v, %v)`, mem_id, t.Format("20060102"), valid, connected, 0, 0);
            if valid == 1 {
                currentIdInsert := fmt.Sprintf(`UPDATE global SET current_id=%v WHERE current_id < %v;`, mem_id, mem_id)
                fmt.Printf("Processed valid Mem %v\n", mem_id)
                dbChan <- currentIdInsert
            }
            dbChan <- memInsert
        }
    }
}

func dbExec(c chan string) {
    var v string
    ok := true

    for ok {
        if v, ok = <-c; ok {
            err := dbConn.Exec(v)

            if err != nil {
                fmt.Println("DB ERROR: ", err, v)
                panic(err)
            }
        }
    }
}

func memProducer(c chan int64) {
    defer close(c)

    nextMemId := currentId

    for true {
        c <- nextMemId
        nextMemId++
    }
}
