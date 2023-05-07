#!/usr/bin/env bash
output=`go build ../server/` || echo $output
output=`go build ../client/` || echo $output
output=`go build ../cmd/` || echo $output
