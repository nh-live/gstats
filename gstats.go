package main

import (
    "fmt"
    "os"
    "net"
    "syscall"
    "bufio"
    "io"
    "time"
    "flag"
    "sync"
    "strings"
    "strconv"
    "io/ioutil"
    "hash/fnv"
    "github.com/rcrowley/goagain"
    "github.com/BurntSushi/toml"
    "github.com/bmizerany/perks/quantile"
    "github.com/bmizerany/perks/histogram"
)

/////////////
// Consts  //
/////////////
const (
    // Server Listening Addr
    CONN_HOST = "localhost"
    CONN_PORT = ":3333"
    CONN_TYPE = "tcp"
    UDP_PORT = ":7777"

    // Backend Server Addr
    GRAPHITE_ADDR = "localhost:2003"

    // Messege types
    TYPE_KV = "kv"
    TYPE_COUNTER = "c"
    TYPE_GAUGE = "g"
    TYPE_TIMER = "ms"
    TYPE_HISTOGRAM = "h"

    // Config for Metric Config
    HISTOGRAM_MAX_BINS = 5


    // MSG_TEMPLATE
    // MSG_TEMPLATE = `{{.metric}} {{.value}} {{.timpstamp}}`
)

var TIMER_TARGETS = []float64{0.50, 0.90, 0.99}

/////////////
// Usage   //
/////////////

func usage() {
    fmt.Fprintln(
        os.Stderr,
        "Usage: gstats -config=<path-to-config-file>",
    )
}

/////////////
// Config  //
/////////////
type commonConfig struct {
    FlushInterval     time.Duration
    TcpPort           string
    UdpPort           string
    GraphiteAddr      string
    NumBuckets        int
    TimerTargets      []float64
    HistogramMaxBins  int
}

type Config struct {
    Common       commonConfig
    // Put any other configs here...
}

func NewConfig() *Config {
    cfg := &Config{
        Common: commonConfig{
            FlushInterval: 60,
            TcpPort: CONN_PORT,
            UdpPort: UDP_PORT,
            GraphiteAddr: GRAPHITE_ADDR,
            NumBuckets: 1,
            HistogramMaxBins: HISTOGRAM_MAX_BINS,
            TimerTargets: TIMER_TARGETS,
        },
    }
    return cfg
}

func ParseConfigFile(file string) (*Config, error) {
    cfg := NewConfig()

    if file != "" {
        bytes, err := ioutil.ReadFile(file)
        if err != nil {
            return nil, err
        }
        body := string(bytes)

        if _, err := toml.Decode(body, cfg); err != nil {
            return nil, err
        }
    }
    return cfg, nil
}

////////////////////
// Hash Function  //
////////////////////

func hash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

func getBucket(s string, numBuckets int) int {
    return int(hash(s)) % numBuckets
}

//////////////////////
// Metric Types     //
//////////////////////

// Counter
type Counter struct {
    count   int
}

func (c *Counter) inc(num int) {
    c.count = c.count + num
}

// Gauge
type Gauge struct {
    value   float64
}

func (g *Gauge) set(val float64) {
    g.value = val
}

// Timer
type Timer struct {
    stream   *quantile.Stream
}

func NewTimer(priors []float64) (t *Timer){
    return &Timer {
        stream: quantile.NewTargeted(priors...),
    }
}

// Histogram
type Histogram struct {
    histogram   *histogram.Histogram
}

func NewHistogram(MaxBins int) (h *Histogram) {
    return &Histogram {
        histogram: histogram.New(MaxBins),
    }
}

// Key Value
type KeyValue struct {

}


/////////////
// Bucket  //
/////////////
type Bucket struct {
    // Data
    couters         map[string]*Counter
    gauges          map[string]*Gauge
    timers          map[string]*Timer
    histograms      map[string]*Histogram
    kvs             map[string]*KeyValue

    // Lock
    counterLock     sync.Mutex
    gaugeLock       sync.Mutex
    timerLock       sync.Mutex
    histogramLock   sync.Mutex
    kvsLock         sync.Mutex

    // Configs
    maxBins         int
    timerTargets    []float64
}


func NewBucket(maxBins int, timerTargets []float64) (*Bucket) {
    return &Bucket{
        couters: make(map[string]*Counter),
        gauges: make(map[string]*Gauge),
        timers: make(map[string]*Timer),
        histograms: make(map[string]*Histogram),
        kvs: make(map[string]*KeyValue),

        // Configs
        maxBins: maxBins,
        timerTargets: timerTargets,
    }
}

func AddCounterSample(bucket *Bucket, key string, val string) {
    value, _ := strconv.Atoi(val)

    bucket.counterLock.Lock()
    defer bucket.counterLock.Unlock()

    if _, ok := bucket.couters[key]; ok {
        // Exists
        bucket.couters[key].inc(value)
        fmt.Println("Received a Counter (exists)!")
    } else {
        // Not Exist
        bucket.couters[key] = &Counter {
            count: value,
        }
        fmt.Println("Received a Counter (dont exists)!")
    }
}

func AddGaugeSample(bucket *Bucket, key string, val string) {
    value, _ := strconv.ParseFloat(val, 64)

    bucket.gaugeLock.Lock()
    defer bucket.gaugeLock.Unlock()

    if _, ok := bucket.gauges[key]; ok {
        // Exists
        bucket.gauges[key].set(value)
        fmt.Println("Received a Gauge (exists)!")
    } else {
        // Not Exist
        bucket.gauges[key] = &Gauge {
            value: value,
        }
        fmt.Println("Received a Gauge (dont exists)!")
    }
}

func AddTimerSample(bucket *Bucket, key string, val string) {
    value, _ := strconv.ParseFloat(val, 64)

    bucket.timerLock.Lock()
    defer bucket.timerLock.Unlock()

    if _, ok := bucket.timers[key]; ok {
        // Exists
        bucket.timers[key].stream.Insert(value)
        fmt.Println("Received a Timer exists!")
    } else{
        // Not Exists
        bucket.timers[key] = NewTimer(bucket.timerTargets)
        bucket.timers[key].stream.Insert(value)
        fmt.Println("Received a Timer (dont exists)!")
    }
}

func AddHistogramSample(bucket *Bucket, key string, val string) {
    value, _ := strconv.ParseFloat(val, 64)

    bucket.histogramLock.Lock()
    defer bucket.histogramLock.Unlock()

    if _, ok := bucket.histograms[key]; ok {
        // Exists
        bucket.histograms[key].histogram.Insert(value)
        fmt.Println("Received a Historam exists!")
    } else{
        // Not Exists
        bucket.histograms[key] = NewHistogram(bucket.maxBins)
        bucket.histograms[key].histogram.Insert(value)
        fmt.Println("Received a Historam (dont exists)!")
    }
}

func AddKVSample(bucket *Bucket, key string, val string) {

}

////////////////
// Data Sink  //
////////////////
type DataSink struct {
    conn               *net.TCPConn
    flushInterval      time.Duration
    numBuckets         int
    maxBins            int
    timerTargets       []float64
    // Needs some more design...
    buckets            []*Bucket
}

func NewDataSink(addr string, flushInterval time.Duration, numBuckets int, maxBins int, timerTargets []float64) (*DataSink, error) {
    // Initialize a Conn to backend server
    tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
    if err != nil {
        fmt.Println("ResolveTCPAddr failed:", err.Error())
        os.Exit(1)
    }

    conn, err := net.DialTCP("tcp", nil, tcpAddr)
    if err != nil {
        fmt.Println("Dial failed:", err.Error())
        os.Exit(1)
    }

    // Initialize a new Bucket
    buckets := make([]*Bucket, numBuckets)
    for i := 0; i < numBuckets; i++ {
        buckets[i] = NewBucket(maxBins, timerTargets)
    }

    // Initialize Data Sink
    sink := DataSink{
        conn: conn,
        flushInterval: flushInterval * time.Second,
        numBuckets: numBuckets,
        buckets: buckets,
        maxBins: maxBins,
        timerTargets: timerTargets,
    }

    // Start flush goroutine
    go sink.handleFlush()

    return &sink, nil
}

func (ds *DataSink) handleFlush() {
    // Init a ticker
    flushTicker := time.NewTicker(ds.flushInterval)
    for {
        <- flushTicker.C
        // Time Now
        now := time.Now()
        epochNow := now.Unix()
        fmt.Println(epochNow)

        // Swap and buffer
        old_buckets := ds.buckets

        buckets := make([]*Bucket, ds.numBuckets)
        for i := 0; i < ds.numBuckets; i++ {
            buckets[i] = NewBucket(ds.maxBins, ds.timerTargets)
        }

        ds.buckets = buckets

        // Spawn a goroutine for flushing
        for i := 0; i < ds.numBuckets; i++ {
            fmt.Println("Flushing Bucket " + strconv.Itoa(i))
            go flushBucket(old_buckets[i], ds.conn, epochNow)
        }
    }
}

func flushCounters(old_bucket *Bucket, conn *net.TCPConn, epochNow int64) {
    fmt.Println("Flush Counters...")
    for k, c := range old_bucket.couters {
        msg := k + " " + strconv.Itoa(c.count) + " " + strconv.Itoa(int(epochNow)) + "\n"
        fmt.Println("Flushing msg: " + msg)
        conn.Write([]byte(msg))
        fmt.Println("Flushed msg: " + msg)
    }
}

func flushGauges(old_bucket *Bucket, conn *net.TCPConn, epochNow int64) {
    fmt.Println("Flush Gauges...")
    for k, g := range old_bucket.gauges {
        msg := k + " " + strconv.FormatFloat(g.value, 'f', -1, 64) + " " + strconv.Itoa(int(epochNow)) + "\n"
        fmt.Println("Flushing msg: " + msg)
        conn.Write([]byte(msg))
        fmt.Println("Flushed msg: " + msg)
    }
}

func flushTimers(old_bucket *Bucket, conn *net.TCPConn, epochNow int64) {
    fmt.Println("Flush Timers...")
    for k, t := range old_bucket.timers {
        for _, p := range old_bucket.timerTargets {
            fmt.Println(p)
            metric_key := k + ".p" + strconv.Itoa(int(p * 100))
            value := t.stream.Query(p)
            msg := metric_key + " " + strconv.FormatFloat(value, 'f', -1, 64) + " " + strconv.Itoa(int(epochNow)) + "\n"
            fmt.Println("Flushing msg: " + msg)
            conn.Write([]byte(msg))
            fmt.Println("Flushed msg: " + msg)
        }
    }
}

func flushHistograms(old_bucket *Bucket, conn *net.TCPConn, epochNow int64) {
    fmt.Println("Flush Histograms...")
    for k, h := range old_bucket.histograms {
        for i, bin := range h.histogram.Bins() {
            // Flush Histogram Count
            metric_key := k + "." + strconv.Itoa(i)
            count := bin.Count
            msg := metric_key + " " + strconv.Itoa(count) + " " + strconv.Itoa(int(epochNow)) + "\n"
            fmt.Println("Flushing msg: " + msg)
            conn.Write([]byte(msg))
            fmt.Println("Flushed msg: " + msg)
        }
    }
}

func flushKVs(old_bucket *Bucket, conn *net.TCPConn, epochNow int64) {

}

func flushBucket(old_bucket *Bucket, conn *net.TCPConn, epochNow int64) {
    // Flush counters
    go flushCounters(old_bucket, conn, epochNow)

    // Flush gauges
    go flushGauges(old_bucket, conn, epochNow)

    // Flush timers
    go flushTimers(old_bucket, conn, epochNow)

    // Flush histograms
    go flushHistograms(old_bucket, conn, epochNow)

    // Flush kvs
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
func acceptTCPConn(l *net.TCPListener, ds *DataSink) {
    for {
        // Listen for an incoming connection.
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
        }
        // Handle connections in a new goroutine.
        go handleRequest(conn, ds)
    }
}

// Handles incoming requests.
func handleRequest(conn net.Conn, ds *DataSink) {
    // Close the connection when you're done with it.
    defer conn.Close()

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

        // Calculate hash key
        hashKey := getBucket(key, ds.numBuckets)

        // Feed into data buckets pool
        switch msgType {
            case TYPE_COUNTER:
                AddCounterSample(ds.buckets[hashKey], key, value)
            case TYPE_HISTOGRAM:
                AddHistogramSample(ds.buckets[hashKey], key, value)
            case TYPE_TIMER:
                AddTimerSample(ds.buckets[hashKey], key, value)
            case TYPE_KV:
                AddKVSample(ds.buckets[hashKey], key, value)
            case TYPE_GAUGE:
                AddGaugeSample(ds.buckets[hashKey], key, value)
        }

        fmt.Println("Processed!")
    }
}

///////////
// Main  //
///////////

func main() {

    // Command line flags
    configFile := flag.String("config", "", "config filename")
    flag.Parse()

    // Parse Config File...
    cfg, err := ParseConfigFile(*configFile)
    if err != nil {
        usage()
        return
    }
    // Initialize Data Sink
    dataSink, err := NewDataSink(cfg.Common.GraphiteAddr, cfg.Common.FlushInterval, cfg.Common.NumBuckets, cfg.Common.HistogramMaxBins, cfg.Common.TimerTargets)

    // TCP
    // Listen for incoming connections.
    l, ppid, err := goagain.GetEnvs()
    if nil != err {
        laddr, err := net.ResolveTCPAddr("tcp", cfg.Common.TcpPort)
        if nil != err {
            os.Exit(1)
        }
        l, err = net.ListenTCP("tcp", laddr)
        if nil != err {
            os.Exit(1)
        }
        go acceptTCPConn(l.(*net.TCPListener), dataSink)
    } else {
        go acceptTCPConn(l.(*net.TCPListener), dataSink)
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
    fmt.Println("TCP Listening on " + cfg.Common.TcpPort)

    // UDP
    udp_addr, err := net.ResolveUDPAddr("udp", cfg.Common.UdpPort)
    if nil != err {
        fmt.Println("Errs out when resolving udp server.")
        os.Exit(1)
    }
    udp_conn, err := net.ListenUDP("udp", udp_addr)

    defer udp_conn.Close()

    fmt.Println("UDP Listening on " + cfg.Common.UdpPort)
    if nil != err {
        fmt.Println("Errs out when starting udp server.")
        os.Exit(1)
    }

    go handleRequest(udp_conn, dataSink)


    // Block the main goroutine awaiting signals.

    if err := goagain.AwaitSignals(l); nil != err {
        os.Exit(1)
    }

    fmt.Println("Going to stop...")
}
