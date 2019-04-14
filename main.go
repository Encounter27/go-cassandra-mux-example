package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

var Session *gocql.Session

func Connect() error {
	var err error

	if Session != nil {
		Session.Close()
		Session = nil
	}

	//cluster := gocql.NewCluster("_portname._tcp.cassandra-headless-svc.default.svc.cluster.local")
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "code2succeed"
	Session, err = cluster.CreateSession()
	return err
}

func init() {
	hystrix.ConfigureCommand("my_command", hystrix.CommandConfig{
		// How long to wait for command to complete, in milliseconds
		Timeout: 5000,

		// MaxConcurrent is how many commands of the same type
		// can run at the same time
		MaxConcurrentRequests: 10,

		// VolumeThreshold is the minimum number of requests
		// needed before a circuit can be tripped due to health
		RequestVolumeThreshold: 10,

		// SleepWindow is how long, in milliseconds,
		// to wait after a circuit opens before testing for recovery
		SleepWindow: 1000,

		// ErrorPercentThreshold causes circuits to open once
		// the rolling measure of errors exceeds this percent of requests
		ErrorPercentThreshold: 50,
	})

	err := Connect()
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

func updateEmp(emp Emp) error {
	fmt.Printf("Updating Emp with id = %s\n", emp.Id)
	if err := Session.Query("UPDATE emps SET firstname = ?, lastname = ?, age = ? WHERE id = ?",
		emp.FirstName, emp.LastName, emp.Age, emp.Id).Exec(); err != nil {
		fmt.Println("Error while updating Emp")
		fmt.Println(err)

		return err
	}

	return nil
}

func GetEmps(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(getEmps())
}

func CreateEmp(w http.ResponseWriter, r *http.Request) {
	var emp Emp
	_ = json.NewDecoder(r.Body).Decode(&emp)

	// Apply circuit breaker for db access
	errChan := hystrix.Go("upsert_cassandra", func() error {
		err := updateEmp(emp)
		if err == gocql.ErrNoConnections {
			//TODO: Retry logic
			err = Connect()
			if err != nil {
				return err
			}

			err = updateEmp(emp) // retry 1
			if err != nil {
				return err
			}
		}
		defer r.Body.Close()

		return nil
	}, func(err error) error {

		json.NewEncoder(w).Encode("Cassandra unavailable")

		return err
	})

	// Block until we have a result or an error.
	select {
	case err := <-errChan:
		log.Println("failure:", err)
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		json.NewEncoder(w).Encode(emp)
	}
}

func main() {
	router := mux.NewRouter()

	router.HandleFunc("/emp", logger(GetEmps)).Methods("GET")
	router.HandleFunc("/emp", logger(CreateEmp)).Methods("POST")

	log.Fatal(http.ListenAndServe(":8080", router))
}

// log is Handler wrapper function for logging
func logger(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.URL.Path, r.Method)
		fn(w, r)
	}
}
