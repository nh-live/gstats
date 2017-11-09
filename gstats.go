package main

import (
    "fmt"
    "os"
    "net"
    "syscall"
    "bufio"
    "io"
    "time"
    "strings"
    "github.com/rcrowley/goagain"
    // "github.com/rcrowley/go-metrics"
)

const (
    // Server Listening Addr
    CONN_HOST = "localhost"
    CONN_PORT = ":3333"
    CONN_TYPE = "tcp"
    UDP_PORT = ":7777"

    // Backend Server Addr
    GRAPHITE_HOST = "localhost"
    GRAPHITE_PORT = ":2003"
)

/////////////
// Usage   //
/////////////

func usage() {
    fmt.Fprintln(
        os.Stderr,
        "Usage: gstats <path-to-config-file>",
    )
}

//////////////////////
// Metric Types     //
//////////////////////
type Counter struct {

}

type Gauge struct {

}

type Timer struct {

}

type Histogram struct {

}

type Bucket struct {
    couters         map[string]Counter
    gauges          map[string]Gauge
    timers          map[string]Timer
    histograms      map[string]Histogram
}


func NewBucket() (*Bucket) {
    return &Bucket{
        couters: make(map[string]Counter),
        gauges: make(map[string]Gauge),
        timers: make(map[string]Timer),
        histograms: make(map[string]Histogram),
    }
}


////////////////
// Data Sink  //
////////////////
type DataSink struct {
    conn               *net.TCPConn
    flushInterval      time.Duration
    // Needs some more design...
    bucket             *Bucket
}

// func NewDataSink(addr string, flushInterval time.Duration) (*DataSink, error) {
//     return
// }

func (ds *DataSink) handleFlush() {
    // Init a ticker
    flushTicker := time.NewTicker(ds.flushInterval)
    for {
        <- flushTicker.C
        // Time Now
        now := time.Now()
        epoch_now := now.Unix()
        fmt.Println(epoch_now)

        // Swap and buffer
        old_bucket := ds.bucket
        ds.bucket = NewBucket()

        // Spawn a goroutine for flushing
        go flushBucket(old_bucket, ds.conn)
    }
}

func flushBucket(old_bucket *Bucket, conn *net.TCPConn) {

}

///////////////////////////
// Parse and Validation  //
///////////////////////////
func parse(buf []byte) (string, string, string) {
    line := string(buf)
    tmp := strings.Split(line, "|")
    kv, msgType := tmp[0], tmp[1]

    tmp = strings.Split(kv, ":")
    key, val := tmp[0], tmp[1]

    return key, val, msgType
}


/////////////
// Server  //
/////////////

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
        key, value, msgType := parse(buf)
        fmt.Println("Key: " + key)
        fmt.Println("Value: " + value)
        fmt.Println("Type: " + msgType)

        // Feed into data buckets pool

        fmt.Println("Processed!")
    }
}

///////////
// Main  //
///////////

func main() {

    // TODO:
    // Parse Config File...


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
    fmt.Println("TCP Listening on " + CONN_PORT)

    // UDP
    udp_addr, err := net.ResolveUDPAddr("udp", UDP_PORT)
    if nil != err {
        fmt.Println("Errs out when resolving udp server.")
        os.Exit(1)
    }
    udp_conn, err := net.ListenUDP("udp", udp_addr)

    defer udp_conn.Close()

    fmt.Println("UDP Listening on " + UDP_PORT)
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
