package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

var Session *gocql.Session

func init() {
	var err error

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "code2succeed"
	Session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	fmt.Println("cassandra init done")
}

type Emp struct {
	Id        string `json:"id,omitempty"`
	FirstName string `json:"firstName,omitempty"`
	LastName  string `json:"lastName,omitempty"`
	Age       int    `json:"age,omitempty"`
}

func getEmps() []Emp {
	fmt.Println("Getting all Employees")
	var emps []Emp
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM emps").Iter()
	for iter.MapScan(m) {
		emps = append(emps, Emp{
			Id:        m["id"].(string),
			FirstName: m["firstname"].(string),
			LastName:  m["lastname"].(string),
			Age:       m["age"].(int),
		})
		m = map[string]interface{}{}
	}

	return emps
}

func updateEmp(emp Emp) {
	fmt.Printf("Updating Emp with id = %s\n", emp.Id)
	if err := Session.Query("UPDATE emps SET firstname = ?, lastname = ?, age = ? WHERE id = ?",
		emp.FirstName, emp.LastName, emp.Age, emp.Id).Exec(); err != nil {
		fmt.Println("Error while updating Emp")
		fmt.Println(err)
	}
}

func GetEmps(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(getEmps())
}

func CreateEmp(w http.ResponseWriter, r *http.Request) {
	var emp Emp
	_ = json.NewDecoder(r.Body).Decode(&emp)
	updateEmp(emp)
	json.NewEncoder(w).Encode(emp)
}

func main() {
	router := mux.NewRouter()

	router.HandleFunc("/emp", GetEmps).Methods("GET")
	router.HandleFunc("/emp", CreateEmp).Methods("POST")
	log.Fatal(http.ListenAndServe(":8080", router))
}
