#!/usr/bin/perl 

package Farqueue;

use strict;
use warnings;

use JSON;
use LWP::UserAgent;

sub new {
    my ($package, $queuename, %args) = @_;

    my $host = $args{Host}||'127.0.0.1';
    my $port = $args{Port}||9094;
    my $requeue = $args{Requeue}||0;

    my $url = "http://$host:$port/$queuename";

    my $json = JSON->new();
    $json->allow_nonref(1);

    my $ua = LWP::UserAgent->new(
        keep_alive => 1,
    );

    return bless {
        ua => $ua,
        url => $url,
        json => $json,
	requeue => $requeue,
    }, $package;
}

sub enqueue {
    my ($self, $data) = @_;

    my $json = $self->{json};
    my $ua = $self->{ua};
    my $url = $self->{url};

    $ua->post($url, Content => $json->encode($data));
}

sub dequeue {
    my ($self, $callback) = @_;

    my $json = $self->{json};
    my $ua = $self->{ua};
    my $url = $self->{url};

    while (1) {
        my $res = $ua->get($url);

        if ($res->code() == 200) {
            my $data = $json->decode($res->content());

            eval {
		$callback->($data);
	    };

	    if ($@) {
		$self->enqueue($data) if $self->{requeue};
		die $@;
	    }

        } elsif ($res->code() == 404) {
            # queue is empty
            sleep 1;
        } else {
            die "Fatal error: " . $res->status_line();
        }
    }
}

1;
