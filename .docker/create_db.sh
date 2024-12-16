#!/usr/bin/env bash

PGPASSWORD=${POSTGRES_PASSWORD} psql -h ${POSTGRES_HOSTNAME} -U ${POSTGRES_USERNAME} -c 'create database opendatacube_test'
