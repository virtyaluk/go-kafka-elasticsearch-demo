FROM golang:1.9 as builder

RUN curl -fsSL -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64 && chmod +x /usr/local/bin/dep

RUN mkdir -p /go/src/github.com/virtyaluk
WORKDIR /go/src/github.com/virtyaluk

ADD . ./

RUN dep ensure -vendor-only
RUN go build -o main .

CMD ["./main"]