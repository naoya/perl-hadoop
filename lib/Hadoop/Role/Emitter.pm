package Hadoop::Role::Emitter;
use strict;
use warnings;

sub new { bless {}, shift }

sub emit {
    my ($self, $key, $value) = @_;
    eval {
        $self->put($key, $value);
    };
    if ($@) {
        warn $@;
    }
}

sub put {
    my ($self, $key, $value) = @_;
    printf "%s\t%s\n", $key, $value;
}

1;

