[![Go Report Card](https://goreportcard.com/badge/github.com/splunk/go-genlib)](https://goreportcard.com/report/github.com/splunk/go-genlib)


# splunk/pipelines

Manage concurrency using Go Generics.

```shell
go get github.com/splunk/pipelines
```

## Overview

`Pipelines` provides a set of primitives for managing concurrent pipelines.
A pipeline is a set of processing stages connected by channels.
Each stage is run on one or more goroutines.
`pipelines` manages starting, stopping, and scaling up each pipeline stage for you.
All you have to do is bring your business logic.


### Primitives

### Features

