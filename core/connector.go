package core

import (
	"bufio"
	"config-center/resource"
	_ "config-center/resource"
	"encoding/json"
	"io"
	"log"
	"net"
)

type Session struct {
	conn net.Conn
	buf map[string]string
}

func (session *Session) receive(key string, value string) {
	session.buf[key] = value
}

func (session *Session) SendAll() {
	data, _ := json.Marshal(session.buf)
	_, err := session.conn.Write(data)
	if err != nil {
		log.Panic(err)
	}
}

type Service struct {
	envMap map[string]*Env
	listener *net.TCPListener
	decoder io.ByteReader
	encoder io.Writer
}

func StartService(addr string) *Service {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal("Start configuration center service has unexpected exception when resolve tcp address: ", err)
		return nil
	}

	listener, listenerErr := net.ListenTCP("tcp", tcpAddr)
	if listenerErr != nil {
		log.Fatal("Start configuration center service has unexpected exception when listening tcp: ", listenerErr)
		return nil
	}
	service := Service{
		envMap: createEnvMap(),
		listener: listener,
	}
	go service.WaitForConn()
	go service.persistAllConfigCache()
	return &service
}

func (service *Service) WaitForConn()  {
	defer service.listener.Close()
	for {
		conn, err := service.listener.AcceptTCP()
		if err != nil {
			log.Println("Error occurs when listening tcp: ", err)
			continue
		}
		log.Println("New connection has been created: ", conn.RemoteAddr().Network())

		err = service.addNewSession(conn)
		if err != nil {
			log.Println("Exception when decode data from client: ", conn.RemoteAddr().String(), ". The error is: ", err)
		}

	}
}

func (service *Service) persistAllConfigCache()  {
	for {
		for key := range service.envMap {
			go service.envMap[key].WriteInDB()
		}
	}
}

func createEnvMap() map[string]*Env {
	db := resource.GetDB(postgres)
	result := make(map[string]*Env)
	// TODO generate query for env
	query := "SELECT"
	rows, err := db.Query(query)
	if err != nil {
		log.Println("Error exits when querying env: ", err)
		return result
	}
	for rows.Next() {
		var envId int
		var envName string
		err = rows.Scan(&envId, &envName)
		if err != nil {
			log.Println("Error exist when scanning row: ", err)
			continue
		}
		result[envName] = &Env{Name: envName}
	}
	return result
}

func (service *Service) getGroupFromClient(req []byte) (*Group, error) {
}

func (service *Service) addNewSession(conn *net.TCPConn) error {
	reader := bufio.NewReader(conn)
	n, _ := conn.ReadFrom(reader)
	if n == 0 {
		log.Println("Client", conn.RemoteAddr().String(), " send empty data")
	}
	buffer := make([]byte, n)
	_, err := reader.Read(buffer)
	if err != nil {
		log.Println("Error when read from connection: ", err)
		return err
	}
	group, e := service.getGroupFromClient(buffer)
	if e != nil {
		return e
	}
	session := &Session{
		conn: conn,
		buf:  make(map[string]string),
	}
	group.sessions = append(group.sessions, session)

	return nil
}