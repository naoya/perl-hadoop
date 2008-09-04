package Hadoop::Mapper;
use strict;
use warnings;
use base qw/Hadoop::Role::Emitter/;

use IO::Handle;

sub run {
    my ($class, $handle) = @_;
    my $self = $class->new;

    if (not defined $handle) {
        $handle = \*STDIN;
    }

    ## FIXME: 入力の形式に併せて処理を変更
    while (my $line = $handle->getline) {
        chomp $line;

        ## SequenceFileAsTextInputFormat
        #my ($key, $value) = split /\t/, $line;

        $self->map(undef, $line);
    }
}

1;
