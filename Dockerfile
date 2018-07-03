# start the build process
FROM golang:latest as builder
WORKDIR /go/src/github.com/acstech/doppler-api/
# copy all the needed files for the program
COPY . .
ENV CGO_ENABLED=0
RUN go build -o ./frontendAPI -ldflags "-s -w" github.com/acstech/doppler-api/cmd/doppler-api/
# move the build file into the final docker image
FROM alpine:latest
COPY --from=builder /go/src/github.com/acstech/doppler-api/frontendAPI /opt/service/
<<<<<<< HEAD
<<<<<<< HEAD
COPY ./entrypoint.sh .
EXPOSE 8000
CMD ["./entrypoint.sh"] 
=======
EXPOSE 8000
CMD ["//opt/service/frontendAPI"] 
>>>>>>> added dockerfile and setup new environment variables for the setup
=======
COPY ./entrypoint.sh .
EXPOSE 8000
CMD ["./entrypoint.sh"] 
>>>>>>> Updated environment variables and added docker files
