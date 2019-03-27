package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
	"net/http"
	"os"
	"os/exec"
	"strconv"
)

func checkError(err error) (uint16, []byte) {
	var errno uint16 = 0
	var errmsg []byte
	if e, ok := err.(*mysql.Error); ok {
		if e.Code > 0 {
			errno = e.Code
			errmsg = e.Msg
		}
	}
	return errno, errmsg
}

func handleHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=x-user-defined")

	user := req.PostFormValue("login")
	pass := req.PostFormValue("password")
	addr := req.PostFormValue("host") + ":" + req.PostFormValue("port")
	db := mysql.New("tcp", "", addr, user, pass)
	err := db.Connect()

	if errno, errmsg := checkError(err); errno > 0 {
		EchoHeader(errno, w)
		w.Write(errmsg)
		return
	}

	dbname := req.PostFormValue("db")
	if dbname != "" {
		err = db.Use(dbname)
		if errno, errmsg := checkError(err); errno > 0 {
			EchoHeader(errno, w)
			w.Write(errmsg)
			return
		}
	}

	EchoHeader(0, w)
	actn := req.PostFormValue("actn")
	if actn == "C" {
		EchoConnInfo(addr, db, w)
		return
	}

	var querys []string
	for key, values := range req.PostForm {
		if key != "q[]" {
			continue
		}
		for _, q := range values {
			querys = append(querys, q)
		}
	}

	if actn == "Q" {
		var errno uint16
		var errmsg []byte

		for i, q := range querys {
			if q == "" {
				return
			}

			affectedrows := 0
			insertid := 0
			numfields := 0
			numrows := 0
			rows, res, err := db.Query(q)
			if errno, errmsg = checkError(err); errno <= 0 {
				if res != nil {
					affectedrows = int(res.AffectedRows())
					insertid = int(res.InsertId())
					numrows = len(rows)
					numfields = len(res.Fields())
				}

				// trick for navicat
				if affectedrows == 0 {
					affectedrows = numrows
				}
			}

			EchoResultSetHeader(errno, affectedrows, insertid, numfields, numrows, w)
			if errno > 0 {
				w.Write([]byte(GetBlock(string(errmsg))))
			} else {
				if numfields > 0 {
					EchoFieldsHeader(res.Fields(), numfields, w)
					EchoData(rows, w)
				} else {
					w.Write([]byte(GetBlock("")))
				}
			}
			if i < len(querys) - 1 {
				w.Write([]byte("\x01"))
			} else {
				w.Write([]byte("\x00"))
			}
		}
	}
}

func EchoHeader(errno uint16, w http.ResponseWriter) {
	str := GetLongBinary(1111)
	str += GetShortBinary(201)
	str += GetLongBinary(int(errno))
	str += GetDummy(6)
	w.Write([]byte(str))
}

func GetLongBinary(num int) string {
	buf := new(bytes.Buffer)
	byteOrder := binary.BigEndian
	binary.Write(buf, byteOrder, uint32(num))
	return buf.String()
}

func GetShortBinary(num int) string {
	buf := new(bytes.Buffer)
	byteOrder := binary.BigEndian
	binary.Write(buf, byteOrder, uint16(num))
	return buf.String()
}

func GetDummy(count int) string {
	var b []byte
	for i := 0; i < count; i ++ {
		b = append(b, 0)
	}
	return string(b)
}

func GetBlock(val string) string {
	buf := new(bytes.Buffer)
	l := len(val)
	if l < 254 {
		binary.Write(buf, binary.BigEndian, uint8(l))
		buf.WriteString(val)
		return buf.String()
	} else {
		return "\xFE" + GetLongBinary(l) + val
	}
}

func EchoConnInfo(addr string, db mysql.Conn, w http.ResponseWriter) {
	rows, res, err := db.Query("SHOW VARIABLES LIKE '%version%'")
	if errno, _ := checkError(err); errno > 0 {
		return
	}

	name := res.Map("Variable_name")
	val := res.Map("Value")

	serverVer := ""
	proto := 0
	for _, row := range rows {
		if row.Str(name) == "version" {
			serverVer = row.Str(val)
		}

		if row.Str(name) == "protocol_version" {
			proto = row.Int(val)
		}
	}

	str := GetBlock(addr + " via TCP/IP")
	str += GetBlock(strconv.Itoa(proto))
	str += GetBlock(serverVer)
	w.Write([]byte(str))
}

func EchoResultSetHeader(errno uint16, affectrows int, insertid int, numfields int, numrows int, w http.ResponseWriter) {
	str := GetLongBinary(int(errno))
	str += GetLongBinary(affectrows)
	str += GetLongBinary(insertid)
	str += GetLongBinary(numfields)
	str += GetLongBinary(numrows)
	str += GetDummy(12)

	w.Write([]byte(str))
}

func EchoFieldsHeader(fields []*mysql.Field, numfields int, w http.ResponseWriter) {
	var str string
	for i := 0; i < numfields; i++ {
		str += GetBlock(fields[i].Name)
		str += GetBlock(fields[i].Table)
		str += GetLongBinary(int(fields[i].Type))
		str += GetLongBinary(int(fields[i].Flags))
		str += GetLongBinary(int(fields[i].DispLen))
	}
	w.Write([]byte(str))
}

func EchoData(rows []mysql.Row, w http.ResponseWriter) {
	for _, row := range rows {
		var str string
		for _, col := range row {
			if col == nil {
				str += "\xFF"
			} else {
				str += GetBlock(string(col.([]byte)))
			}
		}
		w.Write([]byte(str))
	}
}

var port = flag.Int("p", 8080, "set app port with -p=xxx or -p xxx.")
var godaemon = flag.Bool("d", false, "run app as a daemon with -d=true or -d true.")
func init() {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *godaemon {
		cmd := exec.Command(os.Args[0], flag.Args()[1:]...)
		cmd.Start()
		fmt.Printf("%s [PID] %d running...\n", os.Args[0], cmd.Process.Pid)
		*godaemon = false
		os.Exit(0)
	}
}

func main() {
	portStr := strconv.Itoa(*port)
	mux := http.NewServeMux()
	mux.Handle("/tunnel", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleHTTP(w, r)
	}))

	http.ListenAndServe(":" + portStr, mux)
}