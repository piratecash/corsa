package rpc

import (
	"github.com/gofiber/fiber/v3"
)

// ErrorResponse sends a JSON error response with a status code and message.
func ErrorResponse(c fiber.Ctx, statusCode int, message string) error {
	return c.Status(statusCode).JSON(fiber.Map{
		"error": message,
	})
}
