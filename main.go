package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
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
	wsConnMutex sync.Mutex
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
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Error loading .env file "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		log.Fatalf("Error loading .env file: %v", err)
	}

	redisAddr := os.Getenv("REDIS_ADDRESS")
	redisDBStr := os.Getenv("REDIS_DB")
	serverAddress := os.Getenv("PORT")
	fmt.Println("REDIS_ADDR:", redisAddr, "REDIS_DB:", redisDBStr, "SERVER_ADDRESS:", serverAddress)

	// Convert the database number to an integer
	redisDB := 1
	if redisDBStr != "" {
		var err error
		redisDB, err = strconv.Atoi(redisDBStr)
		if err != nil {
			_, file, line, _ := runtime.Caller(0) // Get caller info
			err1 := db.InsertError("Error parsing Redis DB number from environment "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
			if err1 != nil {
				fmt.Println(err1)
			}
			fmt.Println("Invalid Redis DB number:", err)
			os.Exit(1)
		}
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // No password by default
		DB:       redisDB,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Failed to connect to Redis "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		fmt.Println("@@@@@@@@@@Failed to connect to Redis:@@@@@@@@@", err)
		os.Exit(1)
	}
	fmt.Println("Connected to Redis")
	r := gin.Default()
	r.Use(RateLimitMiddleware(300))
	InitializeRedis()
	// Create a group for /websocket
	wsGroup := r.Group("/websocket")

	// Define your WebSocket-related routes under the /websocket group
	wsGroup.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, "Hello World")
	})

	wsGroup.POST("/addInstrument", func(ctx *gin.Context) {
		addInstruments(ctx)
	})

	wsGroup.DELETE("/removeInstrument/:token", func(ctx *gin.Context) {
		removeInstrument(ctx)
	})

	wsGroup.POST("/updateTokenAndKey", func(ctx *gin.Context) {
		updateTokenAndKey(ctx)
	})

	wsGroup.GET("/ws", func(ctx *gin.Context) {
		hub.handleConnection(ctx)
	})

	wsGroup.POST("/quotes", func(ctx *gin.Context) {
		Ltp(ctx)
	})

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
}

func NewHub() *Hub {
	return &Hub{
		clients:           make(map[*Client]bool),
		broadcast:         make(chan []byte),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		userSubscriptions: make(map[*Client]map[string]bool),
		subscribersMutex:  sync.Mutex{},
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
}

func verifyPasetoToken(token string) error {
	var payload map[string]interface{}

	parser := paseto.NewV2()

	secretKey := os.Getenv("TOKEN_SECRET")
	if secretKey == "" {
		secretKey = "a08182838485868788898a8b8c8d8e8f"
	}

	err := parser.Decrypt(token, []byte(secretKey), &payload, nil)
	if err != nil {
		return err
	}
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
				fmt.Println("Error parsing JSON message:", err)
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
}

func (c *Client) handleUpdate(messageData map[string]interface{}) {
	channel, ok := messageData["channel"]
	if !ok {
		return
	}

	channelName, ok := channel.(string)
	if !ok {
		log.Println("Invalid channel name")
		return
	}

	c.subscriptions[channelName] = true
	hub.addSubscription(c, channelName)

	if wsConn != nil {
		channelSubscribers[channelName]++
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
			if err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return
			}

		case <-pingTicker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return
			}
		}
	}
}

func (c *Client) handleMessage(message string) {
	// ctx := context.Background()
	fmt.Println("message:", message)
	message = strings.TrimSpace(message)
	if strings.HasPrefix(message, "subscribe:[") && strings.HasSuffix(message, "]") {
		channelsStr := message[len("subscribe:[") : len(message)-1]
		channels := strings.Split(channelsStr, ",")
		for _, channelName := range channels {
			channelName = strings.TrimSpace(channelName)
			if channelName == "" && len(channels) > 1 {
				continue
			}
			c.subscribe(channelName)
		}
	} else {
		parts := strings.SplitN(message, " ", 2)
		fmt.Println("parts:", parts, len(parts))
		if len(parts) < 0 {
			fmt.Println("Invalid message format:", message)
			return
		}
		channelName := strings.TrimSpace(parts[0])
		c.subscriptions[channelName] = true
		hub.addSubscription(c, channelName)
		if wsConn == nil {
			return
		}
		// Increment the subscriber count for the channel
		channelSubscribers[channelName]++
	}
}

func (c *Client) subscribe(channelName string) {
	fmt.Println("Subscribe called for channel:", channelName)
	ctx := context.Background()
	//check in redis if instrument is present or not
	isMember, err := redisClient.SIsMember(ctx, "instrumentTokens", channelName).Result()
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Error in checking instrument token in redis: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		fmt.Println("Error checking membership:", err)
		return
	}
	if !isMember {
		fmt.Printf("Not allowed to subscribe to channel: %s\n", channelName)
		return
	}
	// hub.subscribersMutex.Lock()
	// defer hub.subscribersMutex.Unlock()
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

func (h *Hub) addSubscription(client *Client, channelName string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.userSubscriptions[client]; !ok {
		h.userSubscriptions[client] = make(map[string]bool)
	}

	h.userSubscriptions[client][channelName] = true
}

func (h *Hub) removeSubscription(client *Client, channelName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.userSubscriptions[client]; ok {
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
	c := context.Background()
	// Create a channel to receive subscription messages
	subscriptionChannel := make(chan *redis.Message)

	// Start a goroutine to handle incoming messages from Redis
	go func() {
		if redisClient == nil {
			return
		}
		// Subscribe to all channels dynamically
		pubsub := redisClient.PSubscribe(c, "*")
		// if pubsub != nil {
		// 	fmt.Println("Failed to subscribe to Redis channels:")
		// 	return
		// }

		if pubsub == nil {
			fmt.Println("Failed to subscribe to Redis channels:")
			return
		}

		// Wait for a confirmation that we are subscribed
		_, err := pubsub.Receive(c)
		if err != nil {
			fmt.Println("Failed to subscribe to Redis channels")
			return
		}

		// Start a goroutine to read subscription messages
		go func() {
			for {
				msg, err := pubsub.ReceiveMessage(c)
				if err != nil {
					fmt.Println("Error receiving message from Redis:", err)
					continue
				}

				// Send the subscription message to the channel
				subscriptionChannel <- msg
			}
		}()

		for {
			select {
			case msg := <-subscriptionChannel:
				// Handle the received message from Redis
				handleRedisMessage(msg.Channel, msg.Payload)
			}
		}
	}()
}

func handleRedisMessage(channel, message string) {
	clientsToDelete := []*Client{} // Create a temporary slice to store clients to delete

	// Iterate over the userSubscriptions map
	for client := range hub.userSubscriptions {
		if client.subscriptions[channel] {
			select {
			case client.send <- []byte(message):
			default:
				close(client.send)
				clientsToDelete = append(clientsToDelete, client) // Add client to the slice for deletion
			}
		}
	}

	// Delete the clients outside of the loop
	for _, client := range clientsToDelete {
		delete(hub.clients, client)
	}
}

// --------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------
func getInstruments(c *gin.Context) {
	instrumentTokens := getInstrumentTokensFromRedis()
	if instrumentTokens == nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Failed to get instrument tokens from Redis ", "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		log.Println("Failed to get instrument tokens from Redis")
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
	ctx := context.Background()

	for _, token := range request.Tokens {
		if err := redisClient.SAdd(ctx, "instrumentTokens", token).Err(); err != nil {
			log.Println("Failed to add instrument token to Redis:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add instruments"})
			return
		}
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
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Failed to get instrument tokens from Redis ", "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		log.Println("Failed to get instrument tokens from Redis")
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

		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Failed to remove instrument token from Redis "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}

		log.Println("Failed to remove instrument token from Redis:", err)
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

	if wsConn != nil {
		err := wsConn.WriteJSON(setModeMessage)
		if err != nil {
			_, file, line, _ := runtime.Caller(0) // Get caller info
			err1 := db.InsertError("Failed to unsubscribe to instruments "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
			if err1 != nil {
				fmt.Println(err1)
			}
			log.Println("Unsubscribe error:", err)
		} else {
			log.Printf("Unsubscribed from instrument: %v\n", instrumentTokens)
		}
	}
}

func updateTokenAndKey(ctx *gin.Context) {
	fmt.Println("Updating token and key...")
	var requestBody struct {
		Token string `json:"token"`
		Key   string `json:"key"`
	}
	if err := ctx.ShouldBindJSON(&requestBody); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if requestBody.Token == "" || requestBody.Key == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Token and key cannot be empty"})
		return
	}
	// Save the token and key to Redis
	err := redisClient.Set(ctx, "zerodha:token", requestBody.Token, 0).Err()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save token to Redis"})
		return
	}

	err = redisClient.Set(ctx, "zerodha:key", requestBody.Key, 0).Err()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save key to Redis"})
		return
	}
	// wsURL := fmt.Sprintf("wss://ws.kite.trade?api_key=%s&access_token=%s", requestBody.Key, requestBody.Token)
	// Update the WebSocket URL with the new token and key

	// // Check if the WebSocket connection is already established
	// if wsConn != nil {
	// 	wsConn.Close() // Close the existing connection
	// }

	// connectToWebSocket(wsURL) // Create a new WebSocket connection with the updated URL
	go connectToWebSocket()

	ctx.JSON(http.StatusOK, gin.H{"message": "Token and Key updated successfully"})
	fmt.Println("Token and Key updated successfully")
	return
}

func connectToWebSocket() *websocket.Conn {
	fmt.Println("Connecting to WebSocket...")
	terminate := make(chan struct{})
	ctx := context.Background()

	// Get the token and key from Redis
	token, err := redisClient.Get(ctx, "zerodha:token").Result()
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Failed to get token for zerodha from Redis: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		fmt.Println("Failed to get token from Redis:", err)
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
	wsURL := fmt.Sprintf("wss://ws.kite.trade?api_key=%s&access_token=%s", key, token)

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

	quoteDataList := parseQuoteData(data)

	for _, quoteData := range quoteDataList {
		publishToRedis(quoteData)
	}
}

func publishToRedis(quoteData QuoteData) {
	jsonData, err := json.Marshal(quoteData)
	if err != nil {
		log.Println("JSON marshaling error: ", err)
		return
	}

	channelName := strconv.FormatUint(uint64(quoteData.Token), 10)

	ctx := context.Background()
	err = redisClient.Publish(ctx, channelName, jsonData).Err()
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err1 := db.InsertError("Redis publish error: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err1 != nil {
			fmt.Println(err1)
		}
		log.Println("Redis publish error: ", err)
		return
	}

	// log.Printf("Published data to Redis channel %s: %s\n", channelName, jsonData)
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

	pktLen := binary.BigEndian.Uint16(data[0:2])

	j := 2
	for i := 0; i < int(pktLen); i++ {
		pLen := binary.BigEndian.Uint16(data[j : j+2])
		pkts = append(pkts, data[j+2:j+2+int(pLen)])

		j = j + 2 + int(pLen)
	}

	for _, pktData := range pkts {
		var quoteData QuoteData
		if len(pktData) >= 44 {
			quoteData.Token = binary.BigEndian.Uint32(pktData[0:4])
			quoteData.LTP = float32(binary.BigEndian.Uint32(pktData[4:8])) / 100
			quoteData.H = float32(binary.BigEndian.Uint32(pktData[32:36])) / 100
			quoteData.L = float32(binary.BigEndian.Uint32(pktData[36:40])) / 100
			quoteData.C = float32(binary.BigEndian.Uint32(pktData[40:44])) / 100
			quoteData.O = float32(binary.BigEndian.Uint32(pktData[28:32])) / 100
			quoteData.V = binary.BigEndian.Uint32(pktData[16:20])

			quoteDataList = append(quoteDataList, quoteData)
		}
	}
	return quoteDataList
}

func getInstrumentTokensFromRedis() []int {
	ctx := context.Background()
	if redisClient == nil {
		return []int{}
	}
	tokenStrs, err := redisClient.SMembers(ctx, "instrumentTokens").Result()
	if err != nil {
		log.Println("Failed to get instrument tokens from Redis:", err)
		return []int{}
	}
	if len(tokenStrs) == 0 {
		return []int{}
	}
	var tokens []int
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

// func subscribeToInstruments(conn *websocket.Conn, instrumentTokens []int) {
// 	fmt.Println("Subscribing to instruments:", instrumentTokens)
// 	// subscribe
// 	subscribeMessage := map[string]interface{}{
// 		"a": "subscribe",
// 		"v": instrumentTokens,
// 	}

// 	err := conn.WriteJSON(subscribeMessage)
// 	if err != nil {
// 		_, file, line, _ := runtime.Caller(0) // Get caller info
// 		err1 := db.InsertError("Subscribe error: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
// 		if err1 != nil {
// 			fmt.Println(err1)
// 		}

// 		log.Fatal("Subscribe error: ", err)
// 	} else {
// 		log.Println("Subscribed to instruments:", instrumentTokens)
// 	}

// 	setModeMessage := map[string]interface{}{
// 		"a": "mode",
// 		"v": []interface{}{"quote", instrumentTokens},
// 	}

// 	err = conn.WriteJSON(setModeMessage)
// 	if err != nil {
// 		_, file, line, _ := runtime.Caller(0) // Get caller info
// 		err1 := db.InsertError("Set mode error: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
// 		if err != nil {
// 			fmt.Println(err1)
// 		}
// 		log.Fatal("Set mode error: ", err)
// 	} else {
// 		log.Println("Subscribed to the following instruments: ", instrumentTokens)
// 	}
// }

type LtpRequest struct {
	Instruments []int `json:"instruments" binding:"required"`
}

func Ltp(c *gin.Context) {
	var req LtpRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ltp, err := redisClient.Get(c, strconv.Itoa(req.Instruments[0])).Result()
	if err == redis.Nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "LTP data not found for the instrument"})
		return
	} else if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error fetching LTP data from Redis"})
		return
	}
	fmt.Println("LTP:", ltp)
	fmt.Println("Request:", req)
	c.JSON(http.StatusOK, gin.H{"message": "Data validated successfully"})
}
