#!/usr/local/bin/perl
use strict;
use warnings;
use FindBin::libs;

package WordCount::Reducer;
use base qw/Hadoop::Reducer/;

sub reduce {
    my ($self, $key, $values) = @_;

    my $count = 0;
    while ( $values->has_next ) {
        $count++;
        $values->next;
    }

    $self->emit( $key => $count );
}

package main;

WordCount::Reducer->run;
