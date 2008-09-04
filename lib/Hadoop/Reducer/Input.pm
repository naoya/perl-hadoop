package Hadoop::Reducer::Input;
use strict;
use warnings;
use base qw/Class::Accessor::Lvalue::Fast/;
use Hadoop::Reducer::Input::Iterator;

__PACKAGE__->mk_accessors(qw/handle buffer/);

sub next_key {
    my $self = shift;
    my $line = $self->buffer ? $self->buffer : $self->next_line;

    if (not defined $line) {
        return;
    }

    my ($key, $value) = split /\t/, $line;
    return $key;
}

sub next_line {
    my $self = shift;

    if ($self->handle->eof) {
        return;
    }

    return $self->buffer = $self->handle->getline;
}

sub getline {
    my $self = shift;

    if (defined $self->buffer) {
        my $buf = $self->buffer;
        $self->buffer = undef;

        return $buf;
    } else {
        return $self->next_line;
    }
}

sub each {
    my $self = shift;
    my $line = $self->getline or return;
    chomp $line;
    split /\t/, $line;
}

sub iterator {
    Hadoop::Reducer::Input::Iterator->new({ input => shift });
}

1;
