// +build !assets

package swaggervalidator

import (
	"os"
	"path/filepath"

	"github.com/getkin/kin-openapi/openapi3"
	"go.uber.org/zap"
)

// findSwaggerPath makes a best-effort to find the path of the swagger file on disk.
// If it can't find the path, it returns the empty string.
func findSwaggerPath(logger *zap.Logger) string {
	// First, look for environment variable pointing at swagger.
	path := os.Getenv("INFLUXDB_VALID_SWAGGER_PATH")
	if path != "" {
		// Environment variable set.
		return path
	}

	logger.Info("INFLUXDB_VALID_SWAGGER_PATH not set; falling back to checking relative paths")

	// Get the path to the executable so we can do a relative lookup.
	execPath, err := os.Executable()
	if err != nil {
		// Give up.
		logger.Info("Can't determine path of currently running executable", zap.Error(err))
		return ""
	}

	execDir := filepath.Dir(execPath)

	// Assume the executable is in bin/$OS/, i.e. the developer built with make.
	path = filepath.Join(execDir, "..", "..", "http", "swagger.yml")
	if _, err := os.Stat(path); err == nil {
		// Looks like we found it.
		return path
	}

	// We didn't build from make... maybe the developer ran something like "go build ./cmd/influxd && ./influxd".
	path = filepath.Join(execDir, "http", "swagger.yml")
	if _, err := os.Stat(path); err == nil {
		// Looks like we found it.
		return path
	}

	// Maybe they're in the influxdb root.
	wd, err := os.Getwd()
	if err == nil {
		path = filepath.Join(wd, "http", "swagger.yml")
		if _, err := os.Stat(path); err == nil {
			// Looks like we found it.
			return path
		}
	}

	logger.Info("Couldn't guess path to swagger definition")
	return ""
}

// loadSwagger makes a best-effort to load swagger from the definition.
// If it can't load it and decode it, loadSwagger returns nil.
func loadSwagger(logger *zap.Logger, swaggerData []byte) *openapi3.Swagger {
	// We most likely don't have swaggerData when building without the asset tag, but check anyway.
	if len(swaggerData) > 0 {
		swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromData(swaggerData)
		if err == nil {
			return swagger
		}

		logger.Info("Tried to load swagger from asset data but failed; continuing to look on disk", zap.Error(err))
	}

	path := findSwaggerPath(logger)
	if path == "" {
		// Already logged an error from findSwaggerPath.
		return nil
	}
	logger.Info("Attempting to load swagger definition", zap.String("path", path))

	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromFile(path)
	if err != nil {
		logger.Info("Tried to load swagger from file but failed", zap.String("path", path), zap.Error(err))
		return nil
	}

	return swagger
}
