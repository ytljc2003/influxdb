// +build assets

package swaggervalidator

import (
	"github.com/getkin/kin-openapi/openapi3"
	"go.uber.org/zap"
)

// loadSwagger attempts to parse swaggerData and returns the parsed Swagger.
// If it can't decode it, loadSwagger returns nil.
func loadSwagger(logger *zap.Logger, swaggerData []byte) *openapi3.Swagger {
	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromData(swaggerData)
	if err != nil {
		logger.Warn("Couldn't decode swagger.yml", zap.Error(err))
		return nil
	}

	return swagger
}
