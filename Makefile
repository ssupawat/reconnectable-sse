.PHONY: deps up down logs demo rebuild clean

deps:
	go mod tidy

up:
	podman-compose up --build -d

down:
	podman-compose down -v

logs:
	podman-compose logs -f api-1 api-2 nginx

rebuild:
	podman-compose build --no-cache
	podman-compose up -d

demo:
	go run ./client

clean:
	podman-compose down -v
	rm -f go.sum
