#!/bin/bash

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOF
  CREATE DATABASE opendatacube_test;
EOF
