
# Go ClickHouse Manager

## Description
`go-ch-manager` is a powerful and efficient tool built to simplify and optimize ClickHouse database management.

Goals
- Simplify ClickHouse administration by providing clear visibility and centralized control
- Quickly identify performance bottlenecks in queries, storage, and resource usage
- Enable faster troubleshooting and optimization with actionable insights and metrics

## Tech Stacks
- Go 1.24+
- ClickHouse
- Go Fiber
- SQLite
- Wails v2 (Desktop App)

## Contact
| Name                   | Email                        | Role    |
| ---------------------- | ---------------------------- | ------- |
| Rahmat Ramadhan Putra  | rahmatrdn.dev@gmail.com     | Creator |


## Development Guide
### Prerequisite
- Git (See [Git Installation](https://git-scm.com/downloads))
- Go 1.24+ (See [Golang Installation](https://golang.org/doc/install))
- ClickHouse (See [ClickHouse Installation](https://clickhouse.com/docs/en/installation/))
- Mockery (Optional) (See [Mockery Installation](https://github.com/vektra/mockery))
- Redis (Optional based on your requirement) (See [Redis Installation](https://redis.io/docs/getting-started/installation/) or use in Docker)

#### Windows OS (for a better development experience)

*   Install [Make](https://www.gnu.org/software/make/) (See [Make Installation](https://leangaurav.medium.com/how-to-setup-install-gnu-make-on-windows-324480f1da69)).


### Installation
1. Clone this repo
```sh
git clone https://github.com/rahmatrdn/go-ch-manager.git
```
2. Copy `.env.example` to `.env`
```sh
cp .env.example .env
```
3. Adjust the `.env` file according to the configuration in your local environment, such as the database, ClickHouse settings, and other configurations
4. Install dependencies
```sh
go mod download
```
5. Start the Application Service
```sh
go run cmd/app/main.go
```
6. Open `http://localhost:7011` in your browser


### Unit test
*tips: if you use `VS Code` as your code editor, you can install extension `golang.go` and follow tutorial [showing code coverage after saving your code](https://dev.to/vuong/golang-in-vscode-show-code-coverage-of-after-saving-test-8g0) to help you create unit test*

- Use [Mockery](https://github.com/vektra/mockery) to generate mock class(es)
```sh
make mock d=DependencyClassName
```
- Run unit test with command below or You can run test per function using Vscode!
```sh
make test
```


### Running In Docker
- Docker Build for APP
```sh
docker build -t go-ch-manager-app:1.0.1 -f ./deploy/docker/app/Dockerfile .
```
- Run docker compose for API and Workers
```sh
docker-compose -f docker-compose.yaml up -d
```


## Desktop App

The desktop app provides a native experience for macOS, Windows, and Linux using [Wails](https://wails.io/).

### Prerequisites
- Go 1.24+
- Wails CLI v2
- Platform-specific requirements:
  - **macOS**: Xcode Command Line Tools (`xcode-select --install`)
  - **Windows**: WebView2 (usually pre-installed on Windows 10/11)
  - **Linux**: `gtk3`, `webkit2gtk` (see [Wails Linux Guide](https://wails.io/docs/gettingstarted/installation#linux))

### Install Wails CLI
```sh
go install github.com/wailsapp/wails/v2/cmd/wails@latest
wails doctor  # Verify installation
```

### Development Mode
Run the desktop app with hot-reload:
```sh
make desktop-dev
```

### Building for Distribution

**Build for current platform:**
```sh
make desktop-build
```

**Build for specific platforms:**
```sh
# macOS (Universal binary - Intel & Apple Silicon)
make desktop-build-darwin

# Windows
make desktop-build-windows

# Linux
make desktop-build-linux
```

The built application will be located in `cmd/desktop/build/bin/`.

### Distributing to Others

1. Build the app for the target platform
2. Compress the output:
   ```sh
   # macOS
   cd cmd/desktop/build/bin
   zip -r "Go-CH-Manager-macOS.zip" go-ch-manager.app

   # Windows - share the .exe file directly
   # Linux - share the binary directly
   ```
3. Share via file sharing service (Google Drive, Dropbox, etc.)

**Note for macOS users:** Since the app is not signed with an Apple Developer certificate, recipients need to:
1. Right-click the app and select "Open"
2. Click "Open" in the security dialog
3. Or go to System Settings > Privacy & Security and click "Open Anyway"

### Automated Builds (GitHub Actions)

The repository includes a GitHub Actions workflow that automatically builds the desktop app for all platforms.

**Trigger a release build:**
```sh
git tag v1.0.0
git push origin v1.0.0
```

This will:
1. Build for macOS (Universal), Windows (amd64), and Linux (amd64)
2. Create a GitHub Release with all binaries attached

**Manual trigger:** You can also trigger builds manually from the Actions tab using "workflow_dispatch".


## Contributing
- Create a new branch with a descriptive name that reflects the changes and switch to the new branch. Use the prefix `feature/` for new features or `fix/` for bug fixes.
```sh
git checkout -b <prefix>/branch-name
```
- Make your change(s) and make the test(s)
- Commit and push your change to upstream repository
```sh
git commit -m "[Type] a meaningful commit message"
git push origin branch-name
```
- Open Merge Request in Repository (Reviewer Check Contact Info)
- Merge Request will be merged only if review phase is passed.

## More Details Information
Contact Creator!
