# Use the official Golang image as a build environment
FROM golang:1.22 as builder

# Set the working directory
WORKDIR /app

# Copy the Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application code
COPY . .

# Build the Go application
RUN go build -o myapp main.go

# Use a minimal image for the runtime
FROM debian:buster-slim

# Set the working directory
WORKDIR /app

# Copy the built application from the builder
COPY --from=builder /app/myapp .

# Expose ports (replace with your app's ports)
EXPOSE 8000 8001 8002 8003

# Command to run the application
CMD ["./myapp"]
