FROM golang:latest 
RUN mkdir /app 
ADD . /app/ 
WORKDIR /app 
EXPOSE 13800
RUN go build -o node . 
CMD ["/app/node"]