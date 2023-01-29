# LaLiga Tech Python Challenge

## Description

The challenge consists on creating a Python tool that is able to parse log
files.

A log file contains newline-terminated, space-separated text formatted like.

For example:

```text
1366815793 gandalf tooler
1366815795 higgins electron
1366815811 sbmaster01 sbrma01
```

Each line represents connection from a host (left) to another host (right) at
a given time.

The lines are roughly sorted by timestamp. They might be out of order by
maximum 5 minutes.

Implement a tool that parse logfiles like these.

The tool should both parse previously written logfiles and terminate or collect
input from a new logfile while it's being written and run indefinitely.

The script will output, once every hour:

1. A list of hostnames connected to a given (configurable) host during the last
  hour
2. A list of hostnames received connections from a given (configurable) host
  during the last hour the hostname that generated most connections in the last
  hour
3. The number of loglines and hostnames can be very high. Consider implementing
  a CPU and memory-efficient solution. Please feel free to make assumptions as
  necessary with proper documentation.

## Links

- [Timestamp To Date Converter](https://timestamp.online/)
- [Clarity code challenge](https://aironman2k.wordpress.com/2021/04/23/clarity-code-challenge/)
- [Spark Log Parser](https://github.com/xiandong79/Spark-Log-Parser)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
- [Loghub](https://github.com/logpai/loghub)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)