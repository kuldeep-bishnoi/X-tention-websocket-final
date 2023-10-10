// db/mongodb.go
package db

import (
	"context"
	"fmt"
	"os"
	"sync"

	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client     *mongo.Client
	collection *mongo.Collection
	errorRepo  *ErrorRepository
	once       sync.Once
)

func InitMongoDB() (*mongo.Client, *mongo.Collection) {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Failed to load .env file:", err)
	}

	// Get MongoDB configuration from environment variables
	mongoURI := os.Getenv("MONGO_URI")
	mongoDatabase := os.Getenv("MONGO_DATABASE")
	mongoCollection := os.Getenv("MONGO_COLLECTION")

	// Set up client options
	clientOptions := options.Client().ApplyURI(mongoURI)

	// Create a new MongoDB client
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		fmt.Println("--------------------Failed to connect to MongoDB--------------------")
		panic("Failed to connect to MongoDB")
	}

	// Test the connection to MongoDB
	err = client.Ping(context.Background(), nil)
	if err != nil {
		fmt.Println("--------------------Failed to connect to MongoDB--------------------")
		panic("Failed to connect to MongoDB")
	}

	fmt.Println("--------------------Connected to MongoDB--------------------")

	// Access the specified database and collection
	database := client.Database(mongoDatabase)
	collection = database.Collection(mongoCollection)

	return client, collection // Return the MongoDB client and collection
}

func InsertError(errorMsg, service, userID, location string, data interface{}) error {
	errorRepo := GetErrorRepository()
	err := errorRepo.InsertError(errorMsg, service, userID, location, data)
	return err
}

// CustomError is a custom error struct for MongoDB errors
type CustomError struct {
	Error     string      `json:"error"`
	Location  string      `json:"location"`
	Service   string      `json:"service"` // The service that generated the error
	UserID    string      `json:"user_id"`
	Data      interface{} `json:"data"`
	Timestamp string      `json:"timestamp"`
}

// ErrorRepository is a repository for handling error documents in MongoDB
type ErrorRepository struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// NewErrorRepository creates a new ErrorRepository
func NewErrorRepository(client *mongo.Client, collection *mongo.Collection) *ErrorRepository {
	return &ErrorRepository{
		client:     client,
		collection: collection,
	}
}

// InsertError inserts an error document into MongoDB
func (repo *ErrorRepository) InsertError(errorMsg string, service string, userID string, location string, data interface{}) error {
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	Mdata := CustomError{
		Error:     errorMsg,
		Location:  location,
		Service:   service,
		UserID:    userID,
		Timestamp: timestamp,
		Data:      data,
	}

	_, err := repo.collection.InsertOne(context.TODO(), Mdata)
	if err != nil {
		fmt.Println("Error inserting error document into MongoDB:", err)
		return err
	}
	return nil
}

func GetErrorRepository() *ErrorRepository {
	once.Do(func() {
		errorRepo = NewErrorRepository(client, collection)
	})
	return errorRepo
}

//steps to use
// _, file, line, _ := runtime.Caller(0) // Get caller info
// 		err := db.InsertError("Error loading .env file ", "websocket", "", file+":"+strconv.Itoa(line))
// 		if err != nil {
// 			fmt.Println(err)
// 		}
