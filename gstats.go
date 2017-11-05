package main

import (
    "fmt"
    "os"
    "net"
    "syscall"
    "bufio"
    "io"
    "time"
    "github.com/rcrowley/goagain"
)

const (
    CONN_HOST = "localhost"
    CONN_PORT = ":3333"
    CONN_TYPE = "tcp"
    UDP_PORT = ":7777"
)


func usage() {
    fmt.Fprintln(
        os.Stderr,
        "Usage: gstats <path-to-config-file>",
    )
}

// Accept TCP Conns
func acceptTCPConn(l *net.TCPListener) {
    for {
        // Listen for an incoming connection.
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
        }
        // Handle connections in a new goroutine.
        go handleRequest(conn)
    }
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
    // Close the connection when you're done with it.
    defer conn.Close()
    // // Make a buffer to hold incoming data.
    // buf := make([]byte, 1024)
    // // Read the incoming connection into the buffer.
    // _, err := conn.Read(buf)
    // if err != nil {
    //     fmt.Println("Error reading:", err.Error())
    // }
    // // Send a response back to person contacting us.
    // conn.Write([]byte("Message received."))

    r := bufio.NewReaderSize(conn, 4096)
    // DONOT block data processing, keep it go fast.
    for {
        buf, _, err := r.ReadLine()
        if err != nil {
            if io.EOF != err {
                fmt.Println("Errs out")
            }
            break
        }
        // Validate and parse incoming data

        // Feed into data buckets pool

        // Testing Example
        conn.Write(buf)

        fmt.Println("Processed!")
    }
}

func main() {

    // TCP
    // Listen for incoming connections.
    l, ppid, err := goagain.GetEnvs()
    if nil != err {
        laddr, err := net.ResolveTCPAddr("tcp", CONN_PORT)
        if nil != err {
            os.Exit(1)
        }
        l, err = net.ListenTCP("tcp", laddr)
        if nil != err {
            os.Exit(1)
        }
        go acceptTCPConn(l.(*net.TCPListener))
    } else {
        go acceptTCPConn(l.(*net.TCPListener))
        if err := goagain.KillParent(ppid); nil != err {
            os.Exit(1)
        }
        for {
            err := syscall.Kill(ppid, 0)
            if err != nil {
                break
            }
            time.Sleep(10 * time.Millisecond)
        }
    }

    // UDP
    udp_addr, err := net.ResolveUDPAddr("udp", UDP_PORT)
    if nil != err {
        fmt.Println("Errs out when resolving udp server.")
        os.Exit(1)
    }
    udp_conn, err := net.ListenUDP("udp", udp_addr)

    defer udp_conn.Close()

    fmt.Println("Listening on " + UDP_PORT)
    if nil != err {
        fmt.Println("Errs out when starting udp server.")
        os.Exit(1)
    }

    go handleRequest(udp_conn)


    // Block the main goroutine awaiting signals.

    if err := goagain.AwaitSignals(l); nil != err {
        os.Exit(1)
    }

    fmt.Println("Going to stop...")
}
