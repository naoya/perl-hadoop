package Hadoop::Reducer::Input::Iterator;
use strict;
use warnings;
use base qw/Class::Accessor::Lvalue::Fast/;

use Hadoop::Reducer::Input::ValuesIterator;

__PACKAGE__->mk_accessors(qw/input current_key/);

sub has_next {
    my $self = shift;
    return if not defined $self->input->next_key;
    1;
}

sub next {
    my $self = shift;

    if ( not defined $self->current_key ) {
        $self->current_key = $self->input->next_key;

        return $self->retval( $self->current_key );
    }

    if ($self->current_key ne $self->input->next_key) {
        $self->current_key = $self->input->next_key;

        return $self->retval( $self->current_key );
    }

    my ($key, $value);
    do {
        ($key, $value) = $self->input->each or return;
    } while ($self->current_key eq $key);

    $self->current_key = $key;

    return $self->retval($key, $value);
}

sub retval {
    my ($self, $key, $value) = @_;
    return (
        $key,
        Hadoop::Reducer::Input::ValuesIterator->new({
            input_iter => $self,
            first      => $value,
        }),
    );
}

1;
