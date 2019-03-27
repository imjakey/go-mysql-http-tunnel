package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	AUTH_ERRNO = 1045
	AUTH_ERRMSG = "authentication error"
)

func checkError(err error) (uint16, string) {
	if err == nil {
		return 0, ""
	}
	var errno uint16
	var errmsg string
	if e, ok := err.(*mysql.Error); ok {
		if e.Code > 0 {
			errno = e.Code
			errmsg = string(e.Msg)
		}
	}

	if err.Error() == AUTH_ERRMSG {
		return AUTH_ERRNO, AUTH_ERRMSG
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
	defer db.Close()
	if errno, errmsg := checkError(err); errno > 0 {
		EchoHeader(errno, GetBlock(errmsg), w)
		return
	}

	dbname := req.PostFormValue("db")
	if dbname != "" {
		err = db.Use(dbname)
		if errno, errmsg := checkError(err); errno > 0 {
			EchoHeader(errno, GetBlock(errmsg), w)
			return
		}
	}

	EchoHeader(0, nil, w)
	actn := req.PostFormValue("actn")
	if actn == "C" {
		EchoConnInfo(addr, db, w)
		return
	}

	if actn == "Q" {
		var errno uint16
		var errmsg string

		querys := getQuerys(req)
		for i, q := range querys {
			if q == "" {
				continue
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
				w.Write(GetBlock(errmsg))
			} else {
				if numfields > 0 {
					EchoFieldsHeader(res.Fields(), numfields, w)
					EchoData(rows, w)
				} else {
					w.Write(GetBlock(""))
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

func getQuerys(req *http.Request) []string {
	var querys []string
	for key, values := range req.PostForm {
		if key != "q[]" {
			continue
		}
		for _, q := range values {
			if enc := req.PostFormValue("encodeBase64"); enc == "1" {
				c := 4 - (len(q) % 4)
				if  c > 0 && c < 4 {
					q += strings.Repeat("=", c)
				}

				data, err := base64.StdEncoding.DecodeString(q)
				if err != nil {
					//fmt.Println(err, q)
					continue
				}
				q = string(data)
			}
			querys = append(querys, q)
		}
	}
	return querys
}

func EchoHeader(errno uint16, msg []byte, w http.ResponseWriter) {
	w.Write(GetLongBinary(1111))
	w.Write(GetShortBinary(201))
	w.Write(GetLongBinary(int(errno)))
	w.Write(GetDummy(6))

	if msg != nil {
		w.Write(msg)
	}
}

func GetLongBinary(num int) []byte {
	buf := new(bytes.Buffer)
	byteOrder := binary.BigEndian
	binary.Write(buf, byteOrder, uint32(num))
	return buf.Bytes()
}

func GetShortBinary(num int) []byte {
	buf := new(bytes.Buffer)
	byteOrder := binary.BigEndian
	binary.Write(buf, byteOrder, uint16(num))
	return buf.Bytes()
}

func GetDummy(count int) []byte {
	var b []byte
	for i := 0; i < count; i ++ {
		b = append(b, 0)
	}
	return b
}

func GetBlock(val string) []byte {
	buf := new(bytes.Buffer)
	l := len(val)
	if l < 254 {
		binary.Write(buf, binary.BigEndian, uint8(l))
		buf.WriteString(val)
	} else {
		buf.Write([]byte("\xFE"))
		buf.Write(GetLongBinary(l))
		buf.WriteString(val)
	}
	return buf.Bytes()
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

	w.Write(GetBlock(addr + " via TCP/IP"))
	w.Write(GetBlock(strconv.Itoa(proto)))
	w.Write(GetBlock(serverVer))
}

func EchoResultSetHeader(errno uint16, affectrows int, insertid int, numfields int, numrows int, w http.ResponseWriter) {
	w.Write(GetLongBinary(int(errno)))
	w.Write(GetLongBinary(affectrows))
	w.Write(GetLongBinary(insertid))
	w.Write(GetLongBinary(numfields))
	w.Write(GetLongBinary(numrows))
	w.Write(GetDummy(12))
}

func EchoFieldsHeader(fields []*mysql.Field, numfields int, w http.ResponseWriter) {
	for i := 0; i < numfields; i++ {
		w.Write(GetBlock(fields[i].Name))
		w.Write(GetBlock(fields[i].Table))
		w.Write(GetLongBinary(int(fields[i].Type)))
		w.Write(GetLongBinary(int(fields[i].Flags)))
		w.Write(GetLongBinary(int(fields[i].DispLen)))
	}
}

func EchoData(rows []mysql.Row, w http.ResponseWriter) {
	for _, row := range rows {
		buf := new(bytes.Buffer)
		for _, col := range row {
			if col == nil {
				buf.Write([]byte("\xFF"))
			} else {
				buf.Write(GetBlock(string(col.([]byte))))
			}
		}
		w.Write(buf.Bytes())
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

	err := http.ListenAndServe(":" + portStr, mux)
	if err != nil {
		fmt.Println("Please check error :")
		fmt.Println(err.Error())
	}
}