#!/usr/bin/env perl
use strict;
use warnings;
use Test::More tests => 2;

BEGIN { use_ok('Hadoop::Mapper') };
BEGIN { use_ok('Hadoop::Reducer') };
