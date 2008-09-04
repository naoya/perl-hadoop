package Hadoop::Reducer;
use strict;
use warnings;
use base qw/Hadoop::Role::Emitter/;

use IO::Handle;
use Hadoop::Reducer::Input;

sub run {
    my $class = shift;
    my $self = $class->new;

    my $input = Hadoop::Reducer::Input->new({ handle => \*STDIN });
    my $it    = $input->iterator;

    while ($it->has_next) {
        my ($key, $values) = $it->next or last;
        eval {
            $self->reduce( $key => $values );
        };
        if ($@) {
            warn $@;
        }
    }
}

1;
