package main

import (
	"fmt"
	"github.com/labstack/echo/v4"
	"main/aws"
	"net/http"
)

func main() {
	if err := aws.InitAws("us-east-2"); err != nil {
		fmt.Println(err.Error())
		return
	}

	e := echo.New()
	str, err := aws.AwsGetParam("test_parameter")
	if err != nil {
		fmt.Println(err.Error())
	}
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, str)
	})
	e.Logger.Fatal(e.Start(":3000"))
}
