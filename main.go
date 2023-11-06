package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"time"
	db "websocket/database"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/o1egl/paseto"
)

var (
	redisClient *redis.Client
	wsURL       string
	// wsConnMutex sync.Mutex
	wsConn      *websocket.Conn
	websocketMu sync.Mutex
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	UserID             int32
	clientCount        int
	hub                *Hub // Central hub for WebSocket connections
	hubMu              sync.Mutex
	ipConnections      map[string]int
	channelSubscribers map[string]int
)

const resetInterval = 1 * 60 * 60

func main() {
	db.InitMongoDB()
	ipConnections = make(map[string]int)
	channelSubscribers = make(map[string]int)

	hub = NewHub()
	go hub.Run()
	if err := godotenv.Load(); err != nil {
		logError("Error loading .env file", err)
		log.Fatalf("Error loading .env file: %v", err)
	}

	redisAddr := os.Getenv("REDIS_ADDRESS")
	redisDBStr := os.Getenv("REDIS_DB")
	serverAddress := os.Getenv("PORT")
	fmt.Println("REDIS_ADDR:", redisAddr, "REDIS_DB:", redisDBStr, "SERVER_ADDRESS:", serverAddress)

	redisDB := 1
	if redisDBStr != "" {
		var err error
		redisDB, err = strconv.Atoi(redisDBStr)
		if err != nil {
			logError("Error parsing Redis DB number from environment", err)
			os.Exit(1)
		}
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // No password by default
		DB:       redisDB,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logError("Failed to connect to Redis", err)
		os.Exit(1)
	}

	r := gin.Default()
	r.Use(RateLimitMiddleware(300))
	InitializeRedis()

	wsGroup := r.Group("/websocket")

	wsGroup.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, "Hello World")
	})

	wsGroup.POST("/addInstrument", addInstruments)

	wsGroup.DELETE("/removeInstrument/:token", removeInstrument)

	wsGroup.POST("/updateTokenAndKey", updateTokenAndKey)

	wsGroup.GET("/ws", hub.handleConnection)

	wsGroup.POST("/quotes", LtpData)

	wsGroup.GET("/getInstruments", getInstruments)

	// Run the Gin server
	if err := r.Run(":" + serverAddress); err != nil {
		log.Fatal("Failed to start Gin server: ", err)
	}

	wsConn = connectToWebSocket()
	if wsConn == nil {
		log.Println("@@@Failed to connect to WebSocket@@@")
	}

	select {}

}

// -------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------

// Client represents a WebSocket client connection.
type Client struct {
	conn          *websocket.Conn
	send          chan []byte
	subscriptions map[string]bool
	ip            string
}

// Hub maintains a set of active clients and broadcasts messages to clients.
type Hub struct {
	clients           map[*Client]bool
	broadcast         chan []byte
	register          chan *Client
	unregister        chan *Client
	userSubscriptions map[*Client]map[string]bool
	subscribersMutex  sync.Mutex
	mu                sync.Mutex
	lastMessage       map[string][]byte //last message
}

func NewHub() *Hub {
	return &Hub{
		clients:           make(map[*Client]bool),
		broadcast:         make(chan []byte),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		userSubscriptions: make(map[*Client]map[string]bool),
		subscribersMutex:  sync.Mutex{},
		lastMessage:       make(map[string][]byte),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			hubMu.Lock()
			h.clients[client] = true
			clientCount++
			fmt.Println("`````````````````````````Connection count:`````````````````````````", clientCount)
			hubMu.Unlock()
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				for channel := range client.subscriptions {
					h.removeSubscription(client, channel)
					// Decrement the subscriber count for the channel
					channelSubscribers[channel]--
					intChannel, err := strconv.Atoi(channel)
					if err != nil {
						fmt.Println("Invalid channel number:", err)
						continue
					}
					// If there are no subscribers, unsubscribe from the instrument
					if channelSubscribers[channel] <= 0 {
						unsubscribeToInstruments(wsConn, []int{intChannel})
					}
				}
				close(client.send)
				delete(h.clients, client)

				// Decrement the connection count for the client's IP address
				ipConnections[client.ip]--
				clientCount--
				fmt.Println("``````````````````Connection count ``````````````````", clientCount)
				// Ensure the count does not go below zero
				if ipConnections[client.ip] < 0 {
					ipConnections[client.ip] = 0
				}
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

func (h *Hub) handleConnection(c *gin.Context) {
	fmt.Println("Connecting...")

	token := c.Query("token")
	if token == "" {
		fmt.Println("Missing token")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing token"})
		return
	}

	if err := verifyPasetoToken(token); err != nil {
		fmt.Println("Invalid token ", err)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	client := &Client{
		conn:          conn,
		send:          make(chan []byte, 256),
		subscriptions: make(map[string]bool),
		ip:            c.ClientIP(),
	}
	h.register <- client
	go client.writePump()
	client.readPump()
	// fmt.Println("client", client.subscriptions)
	// // last message
	// for channel, _ := range client.subscriptions {
	// 	if lastMessage, ok := h.lastMessage[channel]; ok {
	// 		select {
	// 		case client.send <- lastMessage:
	// 			// Message sent successfully.
	// 		default:
	// 			// Handle the case where the client's send channel is full.
	// 			// You can log or handle this according to your needs.
	// 		}
	// 	}
	// }
}

type PasetoClaims struct {
	UserID     int32
	UserName   string
	Email      string
	Expiration time.Time
	NotBefore  time.Time
	IssuedAt   time.Time
}

func verifyPasetoToken(token string) error {
	var payload PasetoClaims

	parser := paseto.NewV2()

	secretKey := os.Getenv("TOKEN_SECRET")
	if secretKey == "" {
		secretKey = "a08182838485868788898a8b8c8d8e8f"
	}

	err := parser.Decrypt(token, []byte(secretKey), &payload, nil)
	if err != nil {
		return err
	}
	UserID = payload.UserID
	// Print the UserID
	fmt.Printf("UserID: %d\n", UserID)
	// If the token is valid, return nil
	return nil
}

func (c *Client) readPump() {
	defer func() {
		hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(1024)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		messageType, p, err := c.conn.ReadMessage()
		if err != nil {
			switch {
			case websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure):
				fmt.Printf("Unexpected close error: %v\n", err)
			case websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure):
				fmt.Printf("Normal close error: %v\n", err)
			default:
				fmt.Printf("WebSocket read error: %v\n", err)
			}
			break
		}

		switch messageType {
		case websocket.TextMessage:
			var messageData map[string]interface{}
			err := json.Unmarshal(p, &messageData)
			if err != nil {
				fmt.Println("Error parsing JSON message:", err.Error())
			} else {
				if message, ok := messageData["type"].(string); ok {
					switch message {
					case "subscribe":
						c.handleSubscribe(messageData)
					case "update":
						c.handleUpdate(messageData)
					default:
						fmt.Println("Discarding unknown message type:", message)
					}
				} else {
					fmt.Println("Message type is missing in the JSON data")
				}
			}
		case websocket.PongMessage:
			fmt.Println("Pong received, connection is alive")
		}
	}
}

func (c *Client) handleSubscribe(messageData map[string]interface{}) {
	channels, ok := messageData["channels"].([]interface{})
	if ok {
		for _, channel := range channels {
			if channelName, ok := channel.(float64); ok {
				channelNamez := int(channelName)
				c.subscribe(strconv.Itoa(channelNamez))
			}
		}
	}
	for channel, _ := range c.subscriptions {
		if lastMessage, ok := hub.lastMessage[channel]; ok {
			select {
			case c.send <- lastMessage:
				// Message sent successfully.
			default:
				// Handle the case where the client's send channel is full.
				// You can log or handle this according to your needs.
			}
		}
	}
}

func (c *Client) handleUpdate(messageData map[string]interface{}) {
	if channel, ok := messageData["channel"].(string); !ok {
		return
	} else {
		//add userid to channel name
		channel = channel + ":" + strconv.Itoa(int(UserID))
		c.subscriptions[channel] = true
		hub.addSubscription(c, channel)
		if wsConn != nil {
			channelSubscribers[channel]++
		}
	}
}

func (c *Client) writePump() {
	defer func() {
		if c.conn != nil {
			c.conn.Close()
		}
	}()

	sendTicker := time.NewTicker(1 * time.Second)
	defer sendTicker.Stop()

	pingTicker := time.NewTicker(10 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.conn.SetWriteDeadline(time.Now().Add(4 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-sendTicker.C:
			{
				err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
				if err != nil {
					return
				}
			}

		case <-pingTicker.C:
			{
				err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
				if err != nil {
					return
				}
			}
		}
	}
}

func (c *Client) subscribe(channelName string) {
	fmt.Println("Subscribe called for channel:", channelName)
	ctx := context.Background()

	// Check if instrument is present in Redis
	isMember, err := redisClient.SIsMember(ctx, "instrumentTokens", channelName).Result()
	if err != nil {
		logError("Error in checking instrument token in Redis", err)
		return
	}
	if !isMember {
		fmt.Printf("Not allowed to subscribe to channel: %s\n", channelName)
		return
	}

	// Check if already subscribed
	if c.subscriptions[channelName] {
		fmt.Printf("Client is already subscribed to channel: %s\n", channelName)
		return
	}
	c.subscriptions[channelName] = true
	hub.addSubscription(c, channelName)

	if wsConn == nil {
		return
	}

	// Increment the subscriber count for the channel
	channelSubscribers[channelName]++

	intChannel, err := strconv.Atoi(channelName)
	if err != nil {
		fmt.Println("Invalid channel number:", err)
		return
	}

	subscribeToInstruments(wsConn, []int{intChannel})
	fmt.Printf("Client subscribed to channel: %s\n", channelName)
}

func logError(message string, err error) {
	_, file, line, _ := runtime.Caller(1) // Get caller info
	err1 := db.InsertError(message+": "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println("Error:", message, err)
}

func (h *Hub) addSubscription(client *Client, channelName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	subscriptions, ok := h.userSubscriptions[client]
	if !ok {
		subscriptions = make(map[string]bool)
		h.userSubscriptions[client] = subscriptions
	}
	subscriptions[channelName] = true
}

func (h *Hub) removeSubscription(client *Client, channelName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs := h.userSubscriptions[client]; subs != nil {
		delete(subs, channelName)
		if len(subs) == 0 {
			delete(h.userSubscriptions, client)
		}
	}
}

func RateLimitMiddleware(limit int) gin.HandlerFunc {
	// Create a map to track the last reset time for each IP
	ipLastReset := sync.Map{}
	ipConnections := sync.Map{}

	return func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "::1" {
			ip = "127.0.0.1"
		}

		// Check if it's time to reset the connection count for this IP
		lastReset, ok := ipLastReset.Load(ip)
		if !ok || time.Since(lastReset.(time.Time)).Seconds() >= resetInterval {
			// Reset the connection count and update the last reset time
			ipConnections.Store(ip, 0)
			ipLastReset.Store(ip, time.Now())
		}

		countValue, _ := ipConnections.LoadOrStore(ip, 0)
		count := countValue.(int)

		if count >= limit {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "Rate limit exceeded"})
			c.Abort()
			return
		}

		ipConnections.Store(ip, count+1)

		defer func() {
			ipConnections.Store(ip, count-1)
		}()

		c.Next()
	}
}

func InitializeRedis() {
	ctx := context.Background()
	subChannel := make(chan *redis.Message)

	go func() {
		if redisClient == nil {
			return
		}
		pubsub := redisClient.PSubscribe(ctx, "*")
		if pubsub == nil {
			log.Println("Failed to subscribe to Redis channels")
			return
		}

		_, err := pubsub.Receive(ctx)
		if err != nil {
			log.Println("Failed to subscribe to Redis channels")
			return
		}

		go func() {
			for msg := range pubsub.Channel() {
				subChannel <- &redis.Message{
					Channel: msg.Channel,
					Payload: msg.Payload,
				}
			}
		}()

		for msg := range subChannel {
			handleRedisMessage(msg.Channel, msg.Payload)
		}
	}()
}

func handleRedisMessage(channel, message string) {
	// Store the last message for the channel
	hub.lastMessage[channel] = []byte(message)

	clientsToDelete := make([]*Client, 0)

	for client := range hub.userSubscriptions {
		if client.subscriptions[channel] {
			select {
			case client.send <- []byte(message):
			default:
				close(client.send)
				clientsToDelete = append(clientsToDelete, client)
			}
		}
	}

	for _, client := range clientsToDelete {
		delete(hub.clients, client)
	}
}

// --------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------
func getInstruments(c *gin.Context) {
	instrumentTokens := getInstrumentTokensFromRedis()
	if instrumentTokens == nil {
		logError("Failed to get instrument tokens from Redis", errors.New("failed to get instrument tokens from Redis"))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get instrument tokens from Redis"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"Instruments": instrumentTokens})
}

func addInstruments(c *gin.Context) {
	var request struct {
		Tokens []int `json:"tokens" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
		return
	}

	redisPipeline := redisClient.Pipeline()
	ctx := context.Background()

	for _, token := range request.Tokens {
		redisPipeline.SAdd(ctx, "instrumentTokens", token)
	}

	_, err := redisPipeline.Exec(ctx)
	if err != nil {
		log.Println("Failed to add instrument tokens to Redis:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add instruments"})
		return
	}

	if wsConn == nil {
		log.Println("@@@Please connect to WebSocket with key and token@@@")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Please connect to WebSocket with key and token"})
		return
	}
	updateWebSocketSubscriptions(wsConn)
	c.JSON(http.StatusOK, gin.H{"message": "Instruments added successfully"})
}

func updateWebSocketSubscriptions(conn *websocket.Conn) {
	instrumentTokens := getInstrumentTokensFromRedis()
	if instrumentTokens == nil {
		logError("Failed to get instrument tokens from Redis", errors.New("Failed to get instrument tokens from Redis"))
		return
	}
	subscribeToInstruments(conn, instrumentTokens)
}

func removeInstrument(c *gin.Context) {
	if wsConn == nil {
		log.Println("@@@Please connect to WebSocket with key and token@@@")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Please connect to WebSocket with key and token"})
		return
	}

	tokenStr := c.Param("token")
	token, err := strconv.Atoi(tokenStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid instrument token"})
		return
	}

	ctx := context.Background()
	if err := redisClient.SRem(ctx, "instrumentTokens", token).Err(); err != nil {
		logError("Failed to remove instrument token from Redis", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to remove instrument"})
		return
	}
	unsubscribeToInstruments(wsConn, []int{token})
	// updateWebSocketSubscriptions(wsConn)
	c.JSON(http.StatusOK, gin.H{"message": "Instrument removed successfully"})
}

func unsubscribeToInstruments(conn *websocket.Conn, instrumentTokens []int) {
	fmt.Println("Unsubscribing to instruments:", instrumentTokens)
	setModeMessage := map[string]interface{}{
		"a": "unsubscribe",
		"v": instrumentTokens,
	}

	if conn != nil {
		err := conn.WriteJSON(setModeMessage)
		if err != nil {
			logError("Failed to unsubscribe to instrument tokens", err)
		} else {
			log.Printf("Unsubscribed from instrument: %v\n", instrumentTokens)
		}
	}
}

func updateTokenAndKey(ctx *gin.Context) {
	fmt.Println("Updating token and key...")
	type RequestBody struct {
		Token string `json:"token"`
		Key   string `json:"key"`
	}

	var requestBody RequestBody
	if err := ctx.ShouldBindJSON(&requestBody); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if requestBody.Token == "" || requestBody.Key == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Token and key cannot be empty"})
		return
	}
	// Save the token and key to Redis
	if err := redisClient.Set(ctx, "zerodha:token", requestBody.Token, 0).Err(); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save token to Redis"})
		return
	}

	if err := redisClient.Set(ctx, "zerodha:key", requestBody.Key, 0).Err(); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save key to Redis"})
		return
	}
	go connectToWebSocket()

	ctx.JSON(http.StatusOK, gin.H{"message": "Token and Key updated successfully"})
}

func connectToWebSocket() *websocket.Conn {
	fmt.Println("Connecting to WebSocket...")
	terminate := make(chan struct{})
	ctx := context.Background()

	// Get the token and key from Redis
	token, err := redisClient.Get(ctx, "zerodha:token").Result()
	if err != nil {
		logError("Failed to get token from Redis", err)
		return nil
	}

	key, err := redisClient.Get(ctx, "zerodha:key").Result()
	if err != nil {
		fmt.Println("Failed to get key from Redis:", err)
		return nil
	}

	// Check if token and key are empty or invalid
	if token == "" || key == "" {
		fmt.Println("Invalid token or key from Redis")
		return nil
	}

	// Initialize WebSocket connection with the retrieved key and token
	wsURL = fmt.Sprintf("wss://ws.kite.trade?api_key=%s&access_token=%s", key, token)

	// If a WebSocket connection already exists, update it with the new URL
	if wsConn != nil {
		err := wsConn.Close()
		if err != nil {
			log.Println("Failed to close existing WebSocket connection:", err)
			return nil
		}
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Println("WebSocket connection failed: ", err)
		return nil
	}

	wsConn = conn

	go func() {
		<-terminate // Terminate the goroutine if termination signal is received
		conn.Close()
	}()

	instrumentTokens := getInstrumentTokensFromRedis()
	subscribeToInstruments(conn, instrumentTokens)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			messageType, p, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseAbnormalClosure) {
					log.Println("WebSocket connection closed gracefully.")
				} else {
					log.Println("WebSocket read error: ", err)
				}
				break
			}
			if messageType == websocket.PongMessage {
				continue
			}
			if messageType == websocket.BinaryMessage {
				handleQuoteData(p)
			}
		}
	}()

	go func() {
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt)
		<-signalCh
		log.Println("Received termination signal. Closing WebSocket and exiting.")
		conn.Close()

		// Send a signal to terminate this goroutine
		os.Exit(0)
	}()

	wg.Wait()

	return conn
}

func handleQuoteData(data []byte) {
	if data == nil {
		return
	}
	quotes := parseQuoteData(data)

	for _, quote := range quotes {
		publishToRedis(quote)
	}
}

func publishToRedis(quoteData QuoteData) {
	jsonData, err := json.Marshal(quoteData)
	if err != nil {
		log.Println("JSON marshaling error:", err)
		return
	}

	channelName := strconv.FormatUint(uint64(quoteData.Token), 10)
	if err := redisClient.Publish(context.Background(), channelName, jsonData).Err(); err != nil {
		logError("Failed to publish data to Redis", err)
		return
	}
}

type QuoteData struct {
	Token uint32
	LTP   float32
	H     float32
	L     float32
	C     float32
	O     float32
	V     uint32
}

func parseQuoteData(data []byte) []QuoteData {
	var quoteDataList []QuoteData
	var pkts [][]byte
	if len(data) < 2 {
		return quoteDataList
	}

	pktLen := binary.BigEndian.Uint16(data[:2])

	j := 2
	for i := 0; i < int(pktLen); i++ {
		pLen := binary.BigEndian.Uint16(data[j : j+2])
		pkts = append(pkts, data[j+2:j+2+int(pLen)])

		j += 2 + int(pLen)
	}

	for _, pktData := range pkts {
		if len(pktData) < 44 {
			continue
		}

		quoteData := QuoteData{
			Token: binary.BigEndian.Uint32(pktData[:4]),
			LTP:   float32(binary.BigEndian.Uint32(pktData[4:8])) / 100,
			H:     float32(binary.BigEndian.Uint32(pktData[32:36])) / 100,
			L:     float32(binary.BigEndian.Uint32(pktData[36:40])) / 100,
			C:     float32(binary.BigEndian.Uint32(pktData[40:44])) / 100,
			O:     float32(binary.BigEndian.Uint32(pktData[28:32])) / 100,
			V:     binary.BigEndian.Uint32(pktData[16:20]),
		}

		quoteDataList = append(quoteDataList, quoteData)
	}

	return quoteDataList
}

func getInstrumentTokensFromRedis() []int {
	if redisClient == nil {
		return []int{}
	}

	ctx := context.Background()
	tokenStrs, err := redisClient.SMembers(ctx, "instrumentTokens").Result()
	if err != nil {
		log.Println("Failed to get instrument tokens from Redis:", err)
		return []int{}
	}

	tokens := make([]int, 0, len(tokenStrs))
	for _, tokenStr := range tokenStrs {
		token, err := strconv.Atoi(tokenStr)
		if err == nil {
			tokens = append(tokens, token)
		}
	}

	return tokens
}

var connMutex sync.Mutex

func subscribeToInstruments(conn *websocket.Conn, instrumentTokens []int) {
	fmt.Println("Subscribing to instruments:", instrumentTokens)

	// Create a buffered channel to queue the messages
	messageQueue := make(chan []byte, 100)

	// Start a goroutine to handle the writes
	go func() {
		for message := range messageQueue {
			connMutex.Lock() // Acquire the lock before writing
			err := conn.WriteMessage(websocket.TextMessage, message)
			connMutex.Unlock() // Release the lock after writing

			if err != nil {
				fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				log.Println("WebSocket write error: ", err)
				return
			}
		}
	}()

	// Subscribe
	subscribeMessage := map[string]interface{}{
		"a": "subscribe",
		"v": instrumentTokens,
	}

	// Convert the subscribeMessage to JSON byte slice
	subscribeJSON, err := json.Marshal(subscribeMessage)
	if err != nil {
		log.Fatal("JSON marshal error: ", err)
	}

	// Queue the message for writing
	messageQueue <- subscribeJSON

	// Set mode
	setModeMessage := map[string]interface{}{
		"a": "mode",
		"v": []interface{}{"quote", instrumentTokens},
	}

	// Convert the setModeMessage to JSON byte slice
	setModeJSON, err := json.Marshal(setModeMessage)
	if err != nil {
		log.Fatal("JSON marshal error: ", err)
	}

	// Queue the message for writing
	messageQueue <- setModeJSON

	// Close the message queue to indicate that no more messages will be queued
	close(messageQueue)

	log.Println("Subscribed to the following instruments:", instrumentTokens)
}

type LtpRequest struct {
	Instruments []int `json:"instruments" binding:"required"`
}

func LtpData(c *gin.Context) {
	var req LtpRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ltpData := make(map[int]float32, len(req.Instruments))

	for _, instrument := range req.Instruments {
		channelName := strconv.Itoa(instrument)
		msg, err := redisClient.Subscribe(c, channelName).ReceiveMessage(c)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "error": err.Error()})
			return
		}

		var quote QuoteData
		if err := json.Unmarshal([]byte(msg.Payload), &quote); err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
			return
		}

		ltpData[instrument] = quote.LTP
	}

	response := gin.H{
		"Data":   ltpData,
		"status": "success",
		"error":  nil,
	}

	fmt.Println("LTP Data:", ltpData)
	fmt.Println("Request:", req)

	c.JSON(http.StatusOK, response)
}
