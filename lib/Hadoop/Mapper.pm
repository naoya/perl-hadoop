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

    ## FIXME: ���Ϥη�����ʻ���ƽ������ѹ�
    while (my $line = $handle->getline) {
        chomp $line;

        ## SequenceFileAsTextInputFormat
        #my ($key, $value) = split /\t/, $line;

        $self->map(undef, $line);
    }
}

1;
