
# Go ClickHouse Manager

<img width="2928" height="2102" alt="image" src="https://github.com/user-attachments/assets/02d35212-d914-415e-a2f3-c03d7ff45b94" />

`go-ch-manager` is a powerful, lightweight, and efficient management tool designed to simplify day-to-day operations of ClickHouse databases at scale.
It provides engineers, data teams, and platform owners with deep visibility into query execution, system performance, storage usage, and cluster health—all from a centralized and easy-to-use interface.

Built with performance and operability in mind, go-ch-manager helps teams move from reactive firefighting to proactive optimization by turning raw ClickHouse system data into actionable insights.

Goals
- Simplify ClickHouse administration by providing clear visibility and centralized control
- Quickly identify performance bottlenecks in queries, storage, and resource usage
- Enable faster troubleshooting and optimization with actionable insights and metrics
- Improve Operational Confidence & Stability

## Tech Stacks
- Go 1.25+
- ClickHouse
- Go Fiber
- SQLite

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
2. Copy `example.env` to `.env`
```sh
cp .env.example .env
```
3. Adjust the `.env` file according to the configuration in your local environment, such as the database, or other settings 
7. Start the Application Service
```sh
go run cmd/app/main.go
```
8. Open `http://localhost:7011` in your browser


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
