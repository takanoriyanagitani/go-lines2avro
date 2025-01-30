#!/bin/sh

geninput(){
	echo any input texts
	echo will be
	echo converted
	echo to
	echo simple avro
	echo file
}

geninput |
	./lines2avro |
	rq -aJ |
	jq -c
