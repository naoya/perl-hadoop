package Hadoop::Reducer::Input::ValuesIterator;
use strict;
use warnings;
use base qw/Class::Accessor::Lvalue::Fast/;

__PACKAGE__->mk_accessors(qw/input_iter first/);

sub has_next {
    my $self = shift;
    return 1 if $self->first;

    if (not defined $self->input_iter->input->next_key) {
        return;
    }

    return $self->input_iter->current_key eq $self->input_iter->input->next_key;
}

sub next {
    my $self = shift;
    if (my $first = $self->first) {
        $self->firs = undef;
        return $first;
    }

    my ($key, $value) = $self->input_iter->input->each;
    return $value;
}

1;
