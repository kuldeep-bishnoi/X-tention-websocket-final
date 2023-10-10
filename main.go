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

	hub                *Hub // Central hub for WebSocket connections
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
		err := db.InsertError("Error loading .env file "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}
		log.Fatalf("Error loading .env file: %v", err)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	redisDBStr := os.Getenv("REDIS_DB")
	serverAddress := os.Getenv("SERVER_ADDRESS")
	fmt.Println("REDIS_ADDR:", redisAddr, "REDIS_DB:", redisDBStr, "SERVER_ADDRESS:", serverAddress)

	// Convert the database number to an integer
	redisDB := 1
	if redisDBStr != "" {
		var err error
		redisDB, err = strconv.Atoi(redisDBStr)
		if err != nil {
			_, file, line, _ := runtime.Caller(0) // Get caller info
			err := db.InsertError("Error parsing Redis DB number from environment "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Invalid Redis DB number:", err)
			os.Exit(1)
		}
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   redisDB,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Failed to connect to Redis "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("@@@@@@@@@@Failed to connect to Redis:@@@@@@@@@", err)
		os.Exit(1)
	}

	r := gin.Default()
	r.Use(RateLimitMiddleware(10))
	InitializeRedis()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, "Hello World")
	})

	r.POST("/addInstrument/:token", func(ctx *gin.Context) {
		addInstrument(ctx)
	})

	r.DELETE("/removeInstrument/:token", func(ctx *gin.Context) {
		removeInstrument(ctx)
	})

	r.POST("/updateTokenAndKey", func(ctx *gin.Context) {
		updateTokenAndKey(ctx)
	})

	r.GET("/ws", func(ctx *gin.Context) {
		hub.handleConnection(ctx)
	})

	r.GET("/getInstruments", getInstruments)

	if err := r.Run(serverAddress); err != nil {
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
	mu                sync.Mutex
}

func NewHub() *Hub {
	return &Hub{
		clients:           make(map[*Client]bool),
		broadcast:         make(chan []byte),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		userSubscriptions: make(map[*Client]map[string]bool),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
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

	// Read and verify the PASETO token from the request header
	token := c.GetHeader("Authorization")
	if token == "" {
		// If the token is missing, return an unauthorized response with an error message
		fmt.Println("Missing token")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing token"})
		return
	}

	// Verify the PASETO token
	if err := verifyPasetoToken(token); err != nil {
		// If the token is invalid, return an unauthorized response with an error message
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
		// fmt.Println("pong")
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		messageType, p, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// Handle unexpected close error (e.g., client closed the connection)
				fmt.Printf("Unexpected close error: %v\n", err)
			} else if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure) {
				// Handle normal close error (e.g., client closed the connection)
				fmt.Printf("Normal close error: %v\n", err)
			} else {
				fmt.Printf("WebSocket read error: %v\n", err)
			}
			break
		}
		if messageType == websocket.TextMessage {
			c.handleMessage(string(p))
		} else if messageType == websocket.PongMessage {
			fmt.Println("Pong received, connection is alive")
		}
	}
}

func (c *Client) writePump() {
	// Create a ticker that ticks every 30 seconds
	ticker := time.NewTicker(30 * time.Second)

	// Create a ticker that ticks every 5 seconds to send ping messages
	pingTicker := time.NewTicker(5 * time.Second)

	// Defer closing the connection and stopping the tickers when the function returns
	defer func() {
		ticker.Stop()
		pingTicker.Stop()
		c.conn.Close()
	}()

	// Infinite loop to continuously handle messages and ping messages
	for {
		select {
		case message, ok := <-c.send:
			// If the channel is closed, write a close message to the connection and return
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Set a write deadline of 10 seconds
			c.conn.SetWriteDeadline(time.Now().Add(4 * time.Second))

			// Write the message to the connection
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			// Write a ping control message to the connection to check the connection status
			if err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return
			}

		case <-pingTicker.C:
			// Write a ping control message to the connection to check the connection status
			if err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return
			}
		}
	}
}

func (c *Client) handleMessage(message string) {
	ctx := context.Background()
	message = strings.TrimSpace(message)
	if strings.HasPrefix(message, "subscribe:{") && strings.HasSuffix(message, "}") {
		channelsStr := message[len("subscribe:{") : len(message)-1]
		channels := strings.Split(channelsStr, ",")
		for _, channelName := range channels {
			channelName = strings.TrimSpace(channelName)
			c.subscribe(channelName)
		}
	} else {
		parts := strings.SplitN(message, " ", 2)
		if len(parts) != 2 {
			fmt.Println("Invalid message format:", message)
			return
		}
		channelName := strings.TrimSpace(parts[0])
		data := parts[1]
		redisClient.Publish(ctx, channelName, data)
	}
}

func (c *Client) subscribe(channelName string) {
	ctx := context.Background()
	//check in redis if instrument is present or not
	isMember, err := redisClient.SIsMember(ctx, "instrumentTokens", channelName).Result()
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Error in checking instrument token in redis: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Error checking membership:", err)
		return
	}
	if !isMember {
		fmt.Printf("Not allowed to subscribe to channel: %s\n", channelName)
		return
	}
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
	if _, ok := h.userSubscriptions[client]; !ok {
		h.userSubscriptions[client] = make(map[string]bool)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	clientSubscriptions, ok := h.userSubscriptions[client]
	if !ok {
		clientSubscriptions = make(map[string]bool)
		h.userSubscriptions[client] = clientSubscriptions
	}
	clientSubscriptions[channelName] = true
}

func (h *Hub) removeSubscription(client *Client, channelName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.userSubscriptions[client], channelName)
	if len(h.userSubscriptions[client]) == 0 {
		delete(h.userSubscriptions, client)
	}
}

func RateLimitMiddleware(limit int) gin.HandlerFunc {
	// Create a map to track the last reset time for each IP
	ipLastReset := make(map[string]time.Time)

	return func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "::1" {
			ip = "127.0.0.1"
		}

		// Check if it's time to reset the connection count for this IP
		lastReset, ok := ipLastReset[ip]
		if !ok || time.Since(lastReset).Seconds() >= resetInterval {
			// Reset the connection count and update the last reset time
			ipConnections[ip] = 0
			ipLastReset[ip] = time.Now()
		}

		count := ipConnections[ip]

		if count >= limit {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "Rate limit exceeded"})
			c.Abort()
			return
		}
		ipConnections[ip]++
		defer func() {
			// Decrement the connection count when the WebSocket connection closes
			ipConnections[ip]--
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
	for client := range hub.userSubscriptions {
		if client.subscriptions[channel] {
			select {
			case client.send <- []byte(message):
			default:
				close(client.send)
				delete(hub.clients, client)
			}
		}
	}
}

// --------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------
func getInstruments(c *gin.Context) {
	instrumentTokens := getInstrumentTokensFromRedis()
	if instrumentTokens == nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Failed to get instrument tokens from Redis ", "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get instrument tokens from Redis"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"Instruments": instrumentTokens})
}

func addInstrument(c *gin.Context) {
	tokenStr := c.Param("token")
	token, err := strconv.Atoi(tokenStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid instrument token"})
		return
	}

	ctx := context.Background()
	if err := redisClient.SAdd(ctx, "instrumentTokens", token).Err(); err != nil {
		log.Println("Failed to add instrument token to Redis:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add instrument"})
		return
	}

	if wsConn == nil {
		log.Println("@@@Please connect to WebSocket with key and token@@@")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Please connect to WebSocket with key and token"})
		return
	}
	updateWebSocketSubscriptions(wsConn)
	c.JSON(http.StatusOK, gin.H{"message": "Instrument added successfully"})
}

func updateWebSocketSubscriptions(conn *websocket.Conn) {
	instrumentTokens := getInstrumentTokensFromRedis()
	if instrumentTokens == nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Failed to get instrument tokens from Redis ", "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
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
		err := db.InsertError("Failed to remove instrument token from Redis "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
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
			err := db.InsertError("Failed to unsubscribe to instruments "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
			if err != nil {
				fmt.Println(err)
			}
			log.Println("Unsubscribe error:", err)
		} else {
			log.Printf("Unsubscribed from instrument: %s\n", instrumentTokens)
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
	return
}

func connectToWebSocket() *websocket.Conn {
	terminate := make(chan struct{})
	ctx := context.Background()
	// Get the token and key from Redis
	token, err := redisClient.Get(ctx, "zerodha:token").Result()
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Failed to get token for zerodha from Redis: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
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
	wsURL = fmt.Sprintf("wss://ws.kite.trade?api_key=%s&access_token=%s", key, token)

	websocketMu.Lock()
	defer websocketMu.Unlock()

	// If a WebSocket connection already exists, update it with the new URL
	if wsConn != nil {
		err := wsConn.Close()
		if err != nil {
			log.Println("Failed to close existing WebSocket connection:", err)
			return nil
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Println("WebSocket connection failed: ", err)
			return nil
		}

		wsConn = conn
		return conn
	}

	for {
		select {
		case <-terminate:
			return nil // Terminate the goroutine if termination signal is received
		default:
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				_, file, line, _ := runtime.Caller(0) // Get caller info
				err := db.InsertError("Failed to connect to WebSocket: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
				if err != nil {
					fmt.Println(err)
				}
				log.Println("WebSocket connection failed: ", err)
				return nil
			}

			// Lock the mutex to update the global WebSocket connection
			wsConnMutex.Lock()
			wsConn = conn
			wsConnMutex.Unlock()

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
		}
	}
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
		err := db.InsertError("Redis publish error: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}
		log.Println("Redis publish error: ", err)
		return
	}

	log.Printf("Published data to Redis channel %s: %s\n", channelName, jsonData)
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

func subscribeToInstruments(conn *websocket.Conn, instrumentTokens []int) {
	fmt.Println("Subscribing to instruments:", instrumentTokens)
	// subscribe
	subscribeMessage := map[string]interface{}{
		"a": "subscribe",
		"v": instrumentTokens,
	}

	err := conn.WriteJSON(subscribeMessage)
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Subscribe error: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}

		log.Fatal("Subscribe error: ", err)
	} else {
		log.Println("Subscribed to instruments:", instrumentTokens)
	}

	setModeMessage := map[string]interface{}{
		"a": "mode",
		"v": []interface{}{"quote", instrumentTokens},
	}

	err = conn.WriteJSON(setModeMessage)
	if err != nil {
		_, file, line, _ := runtime.Caller(0) // Get caller info
		err := db.InsertError("Set mode error: "+err.Error(), "websocket", "", file+":"+strconv.Itoa(line), nil)
		if err != nil {
			fmt.Println(err)
		}
		log.Fatal("Set mode error: ", err)
	} else {
		log.Println("Subscribed to the following instruments: ", instrumentTokens)
	}
}
