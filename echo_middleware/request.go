package middleware

import (
	"github.com/labstack/echo/v4"
	"github.com/lithammer/shortuuid"
)

const RequestIDHeader = "X-Request-Id"

func RequestID() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			requestID := c.Request().Header.Get(RequestIDHeader)
			if requestID == "" {
				requestID = shortuuid.New()
			}

			c.Set(ContextRequestID, requestID)

			return next(c)
		}
	}
}
