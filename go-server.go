package main

import (
	"bytes"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/tidwall/gjson"
)

const (
	DATA_SOCKET_ADDR = "127.0.0.3"
	DATA_SOCKET_PORT = "1998"
	DATA_SOCKET_TYPE = "tcp4"

	AUTH_SOCKET_ADDR = "127.0.0.4"
	AUTH_SOCKET_PORT = "18606"
	AUTH_INIT_KEY    = "myauthkey"
	AUTH_SOCKET_TYPE = "tcp4"
	POLY_URL         = "wss://socket.polygon.io/stocks"
	POLY_MSG         = "T.*,Q.*"
	POLY_CLUSTER     = "/stocks"
	POLY_KEY         = "myapikey"
)

const INTERVAL_MULTIPLIER = 100
const MINIMUM_TRADE_SIZE = 0

var old_auth_key = uuid.UUID{}
var auth_key = uuid.New()

var quotes_map = cmap.New()

type TradeMessage struct {
	Sym string `json:"sym"`
	X   string `json:"x"`
	I   string `json:"i"`
	Z   string `json:"z"`
	P   string `json:"p"`
	BP  string `json:"bp"`
	BS  string `json:"bs"`
	BX  string `json:"bx"`
	AP  string `json:"ap"`
	AS  string `json:"as"`
	AX  string `json:"ax"`
	S   string `json:"s"`
	T   string `json:"t"`
	A   string `json:"a"`
}

type QuoteMessage struct {
	Sym string `json:"sym"`
	Bx  string `json:"bx"`
	Bp  string `json:"bp"`
	Bs  string `json:"bs"`
	Ax  string `json:"ax"`
	Ap  string `json:"ap"`
	As  string `json:"as"`
	T   string `json:"t"`
	Z   string `json:"z"`
}

//https://stackoverflow.com/questions/12677934/create-a-map-of-string-to-list

func recv_poly_data(master_queue chan TradeMessage) {
	// TEST
	// for {
	// 	master_queue <- TradeMessage{
	// 		Sym: "TSLA",
	// 		X:   "NSDQ",
	// 		I:   "5",
	// 		Z:   "2",
	// 		P:   "793.23",
	// 		BP:  "2121",
	// 		BS:  "8282",
	// 		BX:  "9184",
	// 		AP:  "29",
	// 		AS:  "0293",
	// 		AX:  "enas",
	// 		S:   "100299",
	// 		T:   time.Now().Local().String(),
	// 		A:   "as",
	// 	}
	// 	master_queue <- TradeMessage{
	// 		Sym: "AAPL",
	// 		X:   "ARCA",
	// 		I:   "4",
	// 		Z:   "1",
	// 		P:   "19.2",
	// 		BP:  "2121",
	// 		BS:  "8282",
	// 		BX:  "9184",
	// 		AP:  "29",
	// 		AS:  "0293",
	// 		AX:  "enas",
	// 		S:   "1992",
	// 		T:   time.Now().Local().String(),
	// 		A:   "bh",
	// 	}
	// 	time.Sleep(time.Nanosecond)
	// }

	// websocket client stuff
	log.Printf("connecting to %s", POLY_URL)
	c, _, err := websocket.DefaultDialer.Dial(POLY_URL, nil)

	if err != nil {
		log.Println("dial:", err)
	}
	authstr := `{"action": "auth", "params": "` + POLY_KEY + "\"}"
	//fmt.Println(authstr)
	err1 := c.WriteMessage(websocket.TextMessage, []byte(authstr))

	if err1 != nil {
		log.Println("write(recv_poly_data write1):", err1)
		// go recv_poly_data(master_queue)
		// return
	}
	_, message, err := c.ReadMessage()
	if err != nil {
		log.Println("read(recv_poly_data read1):", err)
		//return
	}
	log.Printf("recv: %s", message)
	msgstr := `{"action": "subscribe", "params": "` + POLY_MSG + "\"}"

	err1 = c.WriteMessage(websocket.TextMessage, []byte(msgstr))
	if err1 != nil {
		log.Println("write(recv_poly_data write2):", err1)
		//return
	}

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read(recv_poly_data mainloop):", err)
			time.Sleep(5 * time.Second)
			go recv_poly_data(master_queue)
			return
			//return
		}
		//log.Printf("recv: %s", message)

		parsed_str := gjson.ParseBytes(message)
		if !gjson.Valid(parsed_str.Raw) {
			log.Println("invalid json")
		}
		//log.Println("parsed: " + parsed_str.Raw)
		// #.ev
		parsed_str.ForEach(func(key, data gjson.Result) bool {
			raw_json := data.Raw
			//log.Println(gjson.Get(raw_json, "sym"))
			if gjson.Get(raw_json, "ev").Raw == "\"Q\"" {

				qu := QuoteMessage{
					Sym: strings.Trim(gjson.Get(raw_json, "sym").Raw, "\""),
					Bx:  strings.Trim(gjson.Get(raw_json, "bx").Raw, "\""),
					Bp:  strings.Trim(gjson.Get(raw_json, "bp").Raw, "\""),
					Bs:  strings.Trim(gjson.Get(raw_json, "bs").Raw, "\""),
					Ax:  strings.Trim(gjson.Get(raw_json, "ax").Raw, "\""),
					Ap:  strings.Trim(gjson.Get(raw_json, "ap").Raw, "\""),
					As:  strings.Trim(gjson.Get(raw_json, "as").Raw, "\""),
					T:   strings.Trim(gjson.Get(raw_json, "t").Raw, "\""),
					Z:   strings.Trim(gjson.Get(raw_json, "z").Raw, "\""),
				}

				quotes_map.Set(strings.Trim(gjson.Get(raw_json, "sym").Raw, "\""), qu)

			} else if gjson.Get(raw_json, "ev").Raw == "\"T\"" {

				symbol := strings.Trim(gjson.Get(raw_json, "sym").Raw, "\"")
				exchange := strings.Trim(gjson.Get(raw_json, "x").Raw, "\"")
				tradeid := strings.Trim(gjson.Get(raw_json, "i").Raw, "\"")
				tape := strings.Trim(gjson.Get(raw_json, "z").Raw, "\"")
				price := strings.Trim(gjson.Get(raw_json, "p").Raw, "\"")
				bid := ""
				bidsize := ""
				bidex := ""
				ask := ""
				asksize := ""
				askex := ""
				size := strings.Trim(gjson.Get(raw_json, "s").Raw, "\"")
				timest := strings.Trim(gjson.Get(raw_json, "t").Raw, "\"")

				sizeI, _ := strconv.Atoi(size)
				if sizeI >= MINIMUM_TRADE_SIZE {
					at := "u"
					val, ok := quotes_map.Get(symbol)
					if ok {
						//tmp := val.(string)
						switch v := val.(type) {
						case QuoteMessage:
							if ppf, err := strconv.ParseFloat(price, 32); err == nil {
								bid = v.Bp
								bidsize = v.Bs
								bidex = v.Bx
								ask = v.Ap
								asksize = v.As
								askex = v.Ax
								bp := v.Bp
								ap := v.Ap
								bpf := 0.0
								apf := 0.0
								if bpft, err2 := strconv.ParseFloat(bp, 32); err2 == nil {
									bpf = bpft
								}
								if apft, err2 := strconv.ParseFloat(ap, 32); err2 == nil {
									apf = apft
								}
								bpt := bpf + (bpf * 0.0005)
								apt := apf - (apf * 0.0005)
								// if price was less than bidprice + bidprice*0.0005
								if ppf <= bpt {
									at = "bh"
								} else if ppf >= apt { // else if price was greater than askprice - askprice*0.0005
									at = "as"
								} else { // else trade was in between the spread
									at = "xm"
								}
							}
						}
					}
					//at := "as"
					//at := "xm"
					master_queue <- TradeMessage{
						Sym: symbol,
						X:   exchange,
						I:   tradeid,
						Z:   tape,
						P:   price,
						BP:  bid,
						BS:  bidsize,
						BX:  bidex,
						AP:  ask,
						AS:  asksize,
						AX:  askex,
						S:   size,
						T:   timest,
						A:   at,
					}
				}

			} else {
				//log.Println("received useless data stream")
			}

			return true // keep iterating
		})
		//master_queue <- string(message)
	}
}

// Server portion

// WEBSOCKETTE CLIENT

type Client struct {
	ID     string
	Conn   *websocket.Conn
	Router *Router
	Lock   sync.Mutex
}

func (c *Client) HeartBeat() {

	// Continuosly read client message if any...
	// If the client connection becomes invalid an error is thrown and the client is deleted
loop:
	for {
		_, _, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("client disconnect...", err)
			break loop
		}
	}
	log.Println("remove")
	c.Router.Unregister <- c
}

///////////////

// WEBSOCKETTE ROUTER

type Router struct {
	Register   chan *Client
	Unregister chan *Client
	Clients    map[*Client]bool
	MsgChannel chan TradeMessage
	Lock       *sync.RWMutex
}

func NewRouter(Channel chan TradeMessage) *Router {
	return &Router{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Clients:    make(map[*Client]bool),
		MsgChannel: Channel,
		Lock:       &sync.RWMutex{},
	}
}

func (router *Router) HandleMessage() {
	for {
		sendableArray := make([]TradeMessage, 0)
	timer:
		for timeCh := time.After(INTERVAL_MULTIPLIER * time.Millisecond); ; {
			select {
			case <-timeCh:
				break timer
			case message := <-router.MsgChannel:
				// fill up
				sendableArray = append(sendableArray, message)
			}
		}
		// POTENTIAL RACE CONDITION WITH "A"
		router.Lock.Lock()
		for client, _ := range router.Clients {
			if err := client.Conn.WriteJSON(sendableArray); err != nil {
				log.Println("client message:", err)
			}
		}
		router.Lock.Unlock()
	}
}

func (router *Router) HandleDisconnect() {
	for {
		client := <-router.Unregister
		client.Conn.Close()
		router.Lock.Lock()
		delete(router.Clients, client)
		log.Println("Client removed from router")
		log.Println("Total number of clients: ", len(router.Clients))
		router.Lock.Unlock()
	}
}

func (router *Router) HandleRegister() {
	for {
		client := <-router.Register
		// RACE CONDITION WITH "A"
		router.Lock.Lock()
		router.Clients[client] = true
		log.Println("Now routing messages to new client")
		log.Println("Total number of clients: ", len(router.Clients))
		router.Lock.Unlock()
	}
}

func (router *Router) Start() {

	go router.HandleRegister()
	go router.HandleDisconnect()
	router.HandleMessage()

}

///////////////////////

// WEBSOCKETTE
// WRAPPER FOR GORILLA WEBSOCKET
var w_upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Upgrade HTTP to websocket
func W_Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	// ACCEPT ALL ORIGINS FOR NOW BUT TODO
	w_upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	conn, err := w_upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrrade err:", err)
		return nil, err
	}
	return conn, nil
}

func AddClient(router *Router, w http.ResponseWriter, r *http.Request) {
	log.Println("New client connected to websocket")
	log.Println("Requesting authentication...")
	conn, err := W_Upgrade(w, r)
	if err != nil {
		log.Println("add client err:", err)
	}
	defer conn.Close()

	// add read timeout
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, response, err3 := conn.ReadMessage()
	if err3 != nil {
		log.Println("auth error1", err3)
		return
	}
	// reset time to zero
	conn.SetReadDeadline(time.Time{})
	responseString := strings.TrimRight(string(bytes.Trim(response, "\x00")), "\r\n")
	log.Println(responseString)
	if responseString == strings.TrimRight(auth_key.String(), "\r\n") || responseString == strings.TrimRight(string(old_auth_key.String()), "\r\n") {
		log.Println("Successful wsauth with init key")
	} else {
		log.Println("Authentication error, incorrect or missing key")
		return
	}

	client := &Client{
		Conn:   conn,
		Router: router,
	}

	router.Register <- client
	client.HeartBeat()
}

func setup_server(datastream chan TradeMessage) {
	router := NewRouter(datastream)
	go router.Start()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		AddClient(router, w, r)
	})
}

func handle_auth_client_conn(c net.Conn) {
	log.Println("start handle_auth_client_conn")
	defer c.Close()
	err := c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Println("SetReadDeadline failed:", err)
		return
	}
	c.Write([]byte(string("Send static key to get auth key")))
	max_auth_attempts := 5
	auth_attemps := 0
	buf := make([]byte, 1024)

	for {
		_, err := c.Read(buf)
		if err != nil {
			log.Println("handle client err", err)
			return
		}

		response := strings.TrimRight(string(bytes.Trim(buf, "\x00")), "\r\n")
		if response == strings.TrimRight(AUTH_INIT_KEY, "\r\n") {
			log.Println("Successful auth with init key")
			break
		} else if response == "STOP" || response == "stop" {
			log.Println("Client stopped authentication process")
			return
		} else if auth_attemps >= max_auth_attempts {
			log.Println("Authentication attemps limit reached")
			return
		}
		auth_attemps++
	}

	uuid_to_bytes, err2 := auth_key.MarshalText()
	if err2 != nil {
		log.Println("UUID marshalling error", err2)
		return
	}
	c.Write([]byte(uuid_to_bytes))
}

func refreshAuthKey() {
	s := gocron.NewScheduler(time.UTC)
	s.Every(10).Seconds().Do(func() {
		old_auth_key = auth_key
		auth_key = uuid.New()
	})
	s.StartAsync()
}

func start_key_service() {
	go refreshAuthKey()
	ln, err := net.Listen(AUTH_SOCKET_TYPE, AUTH_SOCKET_ADDR+":"+AUTH_SOCKET_PORT)
	if err != nil {
		log.Println("err start_key_service1", err)
		return
	}
	log.Printf("%s AUTH listening on port %s \n", AUTH_SOCKET_ADDR, AUTH_SOCKET_PORT)
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("err start_key_service2", err)
			return
		}
		go handle_auth_client_conn(conn)
	}
}

func start_server(datastream chan TradeMessage) {
	log.Println("Starting server...")
	go setup_server(datastream)
	http.ListenAndServe(":"+DATA_SOCKET_PORT, nil)
}

func testtask() {
	//fmt.Println("chron")
}

func main() {

	s := gocron.NewScheduler(time.UTC)
	s.Every(5).Seconds().Do(testtask)
	s.StartAsync()
	master_queue := make(chan TradeMessage)
	go recv_poly_data(master_queue)
	go start_server(master_queue)
	start_key_service()

}
