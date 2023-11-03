package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

type LtpRequest struct {
	Instruments []int `json:"instruments" binding:"required"`
}

func Ltp(c *gin.Context) {
	var req LtpRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println("Request:", req)
	c.JSON(http.StatusOK, gin.H{"message": "Data validated successfully"})
}
