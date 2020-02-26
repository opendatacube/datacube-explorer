#!/usr/bin/env bash

PGPASSWORD=${DB_PASSWORD} psql -h ${DB_HOSTNAME} -U ${DB_USERNAME} -c 'create database opendatacube_test'
