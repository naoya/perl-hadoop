#!/usr/bin/env perl
use strict;
use warnings;
use FindBin::libs;

package WordCount::Mapper;
use base qw/Hadoop::Mapper/;

sub map {
    my ($self, $key, $value) = @_;

    for (split /\s+/, $value) {
        $self->emit( $_ => 1 );
    }
}

package main;

WordCount::Mapper->run;
