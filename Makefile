APP_NAME=go-ch-manager
BUILD_DIR=.build
DIST_DIR=$(BUILD_DIR)/dist
MAIN_PACKAGE=./cmd/app
LDFLAGS=-s -w

.PHONY: all clean prepare release

all: release

release: prepare \
	darwin-amd64 darwin-arm64 \
	linux-amd64 linux-arm64 \
	windows-amd64 windows-arm64

prepare:
	mkdir -p $(DIST_DIR)

# =========================
# Darwin
# =========================
darwin-amd64:
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(APP_NAME) $(MAIN_PACKAGE)
	tar -czf $(BUILD_DIR)/$(APP_NAME).darwin-amd64.tar.gz -C $(DIST_DIR) $(APP_NAME)
	rm -f $(DIST_DIR)/$(APP_NAME)

darwin-arm64:
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(APP_NAME) $(MAIN_PACKAGE)
	tar -czf $(BUILD_DIR)/$(APP_NAME).darwin-arm64.tar.gz -C $(DIST_DIR) $(APP_NAME)
	rm -f $(DIST_DIR)/$(APP_NAME)

# =========================
# Linux
# =========================
linux-amd64:
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(APP_NAME) $(MAIN_PACKAGE)
	tar -czf $(BUILD_DIR)/$(APP_NAME).linux-amd64.tar.gz -C $(DIST_DIR) $(APP_NAME)
	rm -f $(DIST_DIR)/$(APP_NAME)

linux-arm64:
	GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(APP_NAME) $(MAIN_PACKAGE)
	tar -czf $(BUILD_DIR)/$(APP_NAME).linux-arm64.tar.gz -C $(DIST_DIR) $(APP_NAME)
	rm -f $(DIST_DIR)/$(APP_NAME)

# =========================
# Windows
# =========================
windows-amd64:
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(APP_NAME).exe $(MAIN_PACKAGE)
	cd $(DIST_DIR) && zip ../../$(BUILD_DIR)/$(APP_NAME).windows-amd64.zip $(APP_NAME).exe
	rm -f $(DIST_DIR)/$(APP_NAME).exe

windows-arm64:
	GOOS=windows GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(APP_NAME).exe $(MAIN_PACKAGE)
	cd $(DIST_DIR) && zip ../../$(BUILD_DIR)/$(APP_NAME).windows-arm64.zip $(APP_NAME).exe
	rm -f $(DIST_DIR)/$(APP_NAME).exe

clean:
	rm -rf $(BUILD_DIR)

# =========================
# Desktop App (Wails)
# =========================
DESKTOP_PACKAGE=./cmd/desktop

.PHONY: desktop-dev desktop-build desktop-build-darwin desktop-build-windows desktop-build-linux

# Run desktop app in development mode
desktop-dev:
	cd cmd/desktop && wails dev

# Build desktop app for current platform
desktop-build:
	cd cmd/desktop && wails build

# Build desktop app for macOS (universal binary)
desktop-build-darwin:
	cd cmd/desktop && wails build -platform darwin/universal

# Build desktop app for Windows
desktop-build-windows:
	cd cmd/desktop && wails build -platform windows/amd64

# Build desktop app for Linux
desktop-build-linux:
	cd cmd/desktop && wails build -platform linux/amd64